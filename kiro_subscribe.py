#!/usr/bin/env python3
"""
Bulk Subscribe Users to Kiro Tiers

Calls the AmazonQDeveloperService.UpdateAssignment API to subscribe,
upgrade, or downgrade AWS Identity Center users to Kiro plans (Pro, Pro+, Power).

UpdateAssignment handles both new subscriptions and changes to existing subscriptions,
so a single API call can create, upgrade, or downgrade as needed.

Usage:
  python kiro_subscribe.py --csv users.csv --region us-east-1
  python kiro_subscribe.py --report report.json --tier pro --region us-east-1
  python kiro_subscribe.py --csv users.csv --tier "pro+" --region us-east-1  # Upgrades existing subscriptions
"""

import argparse
import csv
import json
import logging
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import botocore.auth
import botocore.awsrequest

# ---------------------------------------------------------------------------
# Kiro tier mapping
# ---------------------------------------------------------------------------

# Canonical display name -> API subscription type
TIER_MAP = {
    "Kiro Pro":   "Q_DEVELOPER_STANDALONE_PRO",
    "Kiro Pro+":  "Q_DEVELOPER_STANDALONE_PRO_PLUS",
    "Kiro Power": "Q_DEVELOPER_STANDALONE_POWER",
}

# All accepted aliases -> canonical display name
TIER_ALIASES = {
    "pro":         "Kiro Pro",
    "pro+":        "Kiro Pro+",
    "power":       "Kiro Power",
    "kiro pro":    "Kiro Pro",
    "kiro pro+":   "Kiro Pro+",
    "kiro power":  "Kiro Power",
    "pro_plus":    "Kiro Pro+",
    "proplus":     "Kiro Pro+",
}


def resolve_tier(raw: str) -> tuple[str | None, str]:
    """Resolve a human-friendly tier name to (API subscription type, canonical display name).

    Returns (None, "") if the tier name is not recognized.
    """
    key = raw.strip().lower()
    canonical = TIER_ALIASES.get(key)
    if not canonical:
        # Try exact match on canonical names (case-insensitive)
        for name in TIER_MAP:
            if name.lower() == key:
                canonical = name
                break
    if not canonical:
        return None, ""
    return TIER_MAP[canonical], canonical


# ---------------------------------------------------------------------------
# AWS API
# ---------------------------------------------------------------------------

def update_assignment(
    principal_id: str,
    subscription_type: str,
    credentials,
    region: str,
    principal_type: str = "USER",
    max_retries: int = 5,
) -> tuple[bool, str]:
    """
    Call AmazonQDeveloperService.UpdateAssignment to subscribe, upgrade, or downgrade a user or group.

    UpdateAssignment works for both new subscriptions and modifying existing ones:
    - If the user has no subscription -> creates a new subscription
    - If the user has a different subscription -> upgrades or downgrades to the new tier

    Retries with exponential backoff on ThrottlingException (HTTP 429).
    Returns (success, error_message).
    """
    url = f"https://codewhisperer.{region}.amazonaws.com/"
    body = json.dumps({
        "principalId": principal_id,
        "principalType": principal_type,
        "subscriptionType": subscription_type,
    })
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "AmazonQDeveloperService.UpdateAssignment",
    }

    for attempt in range(max_retries + 1):
        request = botocore.awsrequest.AWSRequest(
            method="POST", url=url, data=body, headers=headers,
        )
        # Note: SigV4 service must be 'q' not 'codewhisperer'
        signer = botocore.auth.SigV4Auth(credentials, "q", region)
        signer.add_auth(request)

        req = urllib.request.Request(
            url, data=body.encode(), headers=dict(request.headers), method="POST",
        )
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            resp.read()
            return True, ""
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode()
            if exc.code == 429 and attempt < max_retries:
                delay = min(30 * 2 ** attempt, 300)
                logging.warning(
                    "Throttled (attempt %d/%d), retrying in %ds...",
                    attempt + 1, max_retries, delay,
                )
                time.sleep(delay)
                continue
            return False, f"HTTP {exc.code}: {error_body}"
        except Exception as exc:
            return False, str(exc)
    return False, "Max retries exceeded"


def get_identity_store_id(session: boto3.Session, region: str | None = None) -> str:
    sso_admin = session.client("sso-admin", region_name=region)
    resp = sso_admin.list_instances()
    instances = resp.get("Instances", [])
    if not instances:
        raise RuntimeError("No AWS Identity Center instance found.")
    return instances[0]["IdentityStoreId"]


def list_all_users(session: boto3.Session, identity_store_id: str, region: str | None = None) -> dict[str, str]:
    """Return {username: user_id} for all users."""
    client = session.client("identitystore", region_name=region)
    users = {}
    paginator = client.get_paginator("list_users")
    for page in paginator.paginate(IdentityStoreId=identity_store_id):
        for user in page.get("Users", []):
            users[user["UserName"]] = user["UserId"]
    return users


# ---------------------------------------------------------------------------
# CSV / report loaders
# ---------------------------------------------------------------------------

def get_users_from_csv(csv_path: Path, default_tier: str | None = None) -> list[dict]:
    """
    Parse CSV. Expects columns: UserName/Email + optional KiroTier/Tier.
    Falls back to default_tier if tier column is missing or empty.
    """
    users = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            norm = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items()}
            username = norm.get("username") or norm.get("user_name") or norm.get("email") or ""
            raw_tier = (
                norm.get("kirotier")
                or norm.get("kiro_tier")
                or norm.get("tier")
                or norm.get("plan")
                or norm.get("kiro_plan")
                or default_tier
                or ""
            )
            if not username:
                logging.warning("Skipping row %d: no username", i)
                continue
            sub_type, tier_label = resolve_tier(raw_tier) if raw_tier else (None, "")
            if not sub_type:
                if default_tier:
                    sub_type, tier_label = resolve_tier(default_tier)
                if not sub_type:
                    logging.warning("Skipping row %d (%s): invalid tier '%s'", i, username, raw_tier)
                    continue
            users.append({"username": username, "subscription_type": sub_type, "tier_label": tier_label})
    return users


def get_users_from_report(report_path: Path, tier: str) -> list[dict]:
    """Read JSON report from idc_manager.py create-users. All users get the same tier."""
    data = json.loads(report_path.read_text())
    created = data.get("created", [])
    sub_type, tier_label = resolve_tier(tier)
    if not sub_type:
        raise ValueError(f"Invalid tier: {tier}")
    return [
        {"username": u["username"], "user_id": u["user_id"], "subscription_type": sub_type, "tier_label": tier_label}
        for u in created
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    tier_help = "Kiro tier: pro ($20/mo), pro+ ($40/mo), power ($200/mo)"

    parser = argparse.ArgumentParser(
        description="Bulk subscribe AWS Identity Center users to Kiro tiers.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Tier names (case-insensitive):
  pro    / kiro pro    ->  Kiro Pro     ($20/mo,  1,000 credits)
  pro+   / kiro pro+   ->  Kiro Pro+    ($40/mo,  2,000 credits)
  power  / kiro power  ->  Kiro Power   ($200/mo, 10,000 credits)

Examples:
  %(prog)s --csv users.csv --region us-east-1
  %(prog)s --csv users.csv --tier pro --region us-east-1
  %(prog)s --report report.json --tier power --region us-east-1
  %(prog)s --report report.json --tier pro+ --workers 10
""",
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--csv", type=Path, help="CSV file with users (may include KiroTier column)")
    input_group.add_argument("--report", type=Path, help="JSON report from idc_manager.py create-users")

    parser.add_argument("--tier", "-t", help=f"{tier_help}. Required with --report, optional default with --csv")
    parser.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--profile", "-p", help="AWS CLI named profile")
    parser.add_argument("--workers", "-w", type=int, default=5, help="Number of parallel workers (default: 5)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.report and not args.tier:
        parser.error("--tier is required when using --report")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    credentials = session.get_credentials().get_frozen_credentials()

    # ---- load users -------------------------------------------------------
    if args.report:
        if not args.report.exists():
            logging.error("Report file not found: %s", args.report)
            sys.exit(1)
        users = get_users_from_report(args.report, args.tier)
    else:
        if not args.csv.exists():
            logging.error("CSV file not found: %s", args.csv)
            sys.exit(1)
        users = get_users_from_csv(args.csv, args.tier)

    if not users:
        logging.error("No valid users to subscribe")
        sys.exit(1)

    # Resolve user_ids for users that don't have them
    needs_resolve = [u for u in users if "user_id" not in u]
    if needs_resolve:
        logging.info("Resolving %d username(s) to UserIds...", len(needs_resolve))
        id_store = get_identity_store_id(session, args.region)
        all_users = list_all_users(session, id_store, args.region)
        resolved = []
        for u in users:
            if "user_id" in u:
                resolved.append(u)
                continue
            uid = all_users.get(u["username"])
            if uid:
                u["user_id"] = uid
                resolved.append(u)
            else:
                logging.warning("User '%s' not found in Identity Center, skipping", u["username"])
        users = resolved

    if not users:
        logging.error("No users to process after resolving UserIds")
        sys.exit(1)

    # Show plan
    tier_summary = {}
    for u in users:
        label = u["tier_label"]
        tier_summary[label] = tier_summary.get(label, 0) + 1
    for label, count in tier_summary.items():
        logging.info("  %s: %d user(s)", label, count)
    logging.info(
        "Will subscribe %d user(s) with %d worker(s)",
        len(users), min(args.workers, len(users)),
    )

    # ---- subscribe in parallel --------------------------------------------
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []

    def _subscribe(user: dict) -> tuple[str, bool, str]:
        ok, err = update_assignment(
            user["user_id"], user["subscription_type"], credentials, args.region,
        )
        return user["username"], ok, err

    with ThreadPoolExecutor(max_workers=min(args.workers, len(users))) as pool:
        futures = {pool.submit(_subscribe, u): u["username"] for u in users}
        for fut in as_completed(futures):
            username, ok, err = fut.result()
            if ok:
                logging.info("Subscribed: %s", username)
                succeeded.append(username)
            else:
                logging.error("Failed for %s: %s", username, err)
                failed.append((username, err))

    # ---- summary ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("KIRO SUBSCRIPTION SUMMARY")
    print("=" * 60)
    print(f"  Subscribed : {len(succeeded)}")
    print(f"  Failed     : {len(failed)}")
    print("=" * 60)

    if failed:
        print("\nFailed users:")
        for uname, err in failed:
            print(f"  - {uname}: {err}")
        sys.exit(1)

    logging.info("Done.")


if __name__ == "__main__":
    main()
