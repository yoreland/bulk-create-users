#!/usr/bin/env python3
"""
AWS Identity Center Manager

A CLI tool for managing AWS Identity Center users:
  - create-users          : Bulk create users from a CSV file
  - reset-password        : Send password reset emails to users
  - enrich                : Enrich a Kiro subscription list with Identity Center user details
  - export-subscriptions  : Export Kiro subscriptions with enriched user details
  - export-store          : Export all users, groups, and memberships from an Identity Store
  - import-store          : Import users, groups, and memberships into another Identity Store

Usage:
  python idc_manager.py create-users users.csv
  python idc_manager.py create-users users.csv --reset-password
  python idc_manager.py reset-password --csv users.csv
  python idc_manager.py enrich kiro-subscriptions.csv -o users.csv
  python idc_manager.py export-subscriptions -o users.csv
  python idc_manager.py export-store -o store-backup.json
  python idc_manager.py import-store store-backup.json --identity-store-id d-xxxxxxxxxx
"""

import argparse
import csv
import json
import logging
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import boto3
import botocore.auth
import botocore.awsrequest
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class UserRecord:
    """A single user row parsed from the CSV."""
    username: str
    given_name: str
    family_name: str
    display_name: str
    email: str
    group_names: list[str] = field(default_factory=list)

    @classmethod
    def from_row(cls, row: dict) -> "UserRecord":
        """Build a UserRecord from a CSV DictReader row.

        Supported column names (case-insensitive, stripped):
          UserName / username / user_name
          GivenName / given_name / first_name / firstname
          FamilyName / family_name / last_name / lastname
          DisplayName / display_name / displayname
          Email / email / email_address
          Groups / groups / group_names  (comma-separated)
        """
        norm = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items()}

        username = (
            norm.get("username")
            or norm.get("user_name")
            or norm.get("email")
            or ""
        )
        given_name = (
            norm.get("givenname")
            or norm.get("given_name")
            or norm.get("first_name")
            or norm.get("firstname")
            or ""
        )
        family_name = (
            norm.get("familyname")
            or norm.get("family_name")
            or norm.get("last_name")
            or norm.get("lastname")
            or ""
        )
        display_name = (
            norm.get("displayname")
            or norm.get("display_name")
            or f"{given_name} {family_name}".strip()
        )
        email = (
            norm.get("email")
            or norm.get("email_address")
            or ""
        )
        raw_groups = (
            norm.get("groups")
            or norm.get("group_names")
            or ""
        )
        groups = [g.strip() for g in raw_groups.split(",") if g.strip()]

        if not username:
            raise ValueError("Missing required field: UserName / Email")
        if not email:
            raise ValueError(f"Missing required field: Email (row: {username})")

        return cls(
            username=username,
            given_name=given_name,
            family_name=family_name,
            display_name=display_name,
            email=email,
            group_names=groups,
        )


# ---------------------------------------------------------------------------
# AWS Helpers -- Identity Store
# ---------------------------------------------------------------------------

def get_identity_store_id(session: boto3.Session, region: str | None = None) -> tuple[str, str]:
    """Return (identity_store_id, instance_arn) from the first SSO instance."""
    sso_admin = session.client("sso-admin", region_name=region)
    resp = sso_admin.list_instances()
    instances = resp.get("Instances", [])
    if not instances:
        raise RuntimeError(
            "No AWS Identity Center instance found. "
            "Make sure Identity Center is enabled in this account/region."
        )
    inst = instances[0]
    return inst["IdentityStoreId"], inst["InstanceArn"]


def create_user(client, identity_store_id: str, user: UserRecord) -> dict:
    """Create a single user in the identity store. Returns API response."""
    params: dict = {
        "IdentityStoreId": identity_store_id,
        "UserName": user.username,
        "Name": {
            "GivenName": user.given_name or user.username,
            "FamilyName": user.family_name or user.username,
        },
        "DisplayName": user.display_name or user.username,
        "Emails": [
            {
                "Value": user.email,
                "Type": "Work",
                "Primary": True,
            }
        ],
    }
    return client.create_user(**params)


def resolve_group_id(client, identity_store_id: str, group_name: str, cache: dict) -> str | None:
    """Resolve a group display name to a GroupId. Uses a cache dict."""
    if group_name in cache:
        return cache[group_name]
    try:
        resp = client.get_group_id(
            IdentityStoreId=identity_store_id,
            AlternateIdentifier={
                "UniqueAttribute": {
                    "AttributePath": "displayName",
                    "AttributeValue": group_name,
                }
            },
        )
        gid = resp["GroupId"]
        cache[group_name] = gid
        return gid
    except ClientError:
        cache[group_name] = None
        return None


def add_user_to_group(client, identity_store_id: str, group_id: str, user_id: str) -> None:
    """Add a user to a group (idempotent -- ignores ConflictException)."""
    try:
        client.create_group_membership(
            IdentityStoreId=identity_store_id,
            GroupId=group_id,
            MemberId={"UserId": user_id},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConflictException":
            pass  # already a member
        else:
            raise


def list_all_users(session: boto3.Session, identity_store_id: str, region: str | None = None) -> dict[str, str]:
    """Return a dict of {username: user_id} for all users in the identity store."""
    client = session.client("identitystore", region_name=region)
    users = {}
    paginator = client.get_paginator("list_users")
    for page in paginator.paginate(IdentityStoreId=identity_store_id):
        for user in page.get("Users", []):
            users[user["UserName"]] = user["UserId"]
    return users


def list_all_users_full(session: boto3.Session, identity_store_id: str, region: str | None = None) -> dict[str, dict]:
    """Return {username: {user_id, given_name, family_name, display_name, email}} for all users."""
    client = session.client("identitystore", region_name=region)
    users = {}
    paginator = client.get_paginator("list_users")
    for page in paginator.paginate(IdentityStoreId=identity_store_id):
        for user in page.get("Users", []):
            name = user.get("Name", {})
            emails = user.get("Emails", [])
            primary_email = ""
            for e in emails:
                if e.get("Primary"):
                    primary_email = e.get("Value", "")
                    break
            if not primary_email and emails:
                primary_email = emails[0].get("Value", "")
            users[user["UserName"]] = {
                "user_id": user["UserId"],
                "given_name": name.get("GivenName", ""),
                "family_name": name.get("FamilyName", ""),
                "display_name": user.get("DisplayName", ""),
                "email": primary_email,
            }
    return users


def list_user_groups(client, identity_store_id: str, user_id: str) -> list[str]:
    """Return a list of group display names for a user."""
    group_names = []
    paginator = client.get_paginator("list_group_memberships_for_member")
    for page in paginator.paginate(
        IdentityStoreId=identity_store_id,
        MemberId={"UserId": user_id},
    ):
        for membership in page.get("GroupMemberships", []):
            group_id = membership.get("GroupId", "")
            if group_id:
                try:
                    resp = client.describe_group(
                        IdentityStoreId=identity_store_id,
                        GroupId=group_id,
                    )
                    group_names.append(resp.get("DisplayName", group_id))
                except ClientError:
                    group_names.append(group_id)
    return group_names


# ---------------------------------------------------------------------------
# AWS Helpers -- Export / Import Store
# ---------------------------------------------------------------------------

def export_all_users(client, identity_store_id: str) -> list[dict]:
    """Export all users with full profile details."""
    users = []
    paginator = client.get_paginator("list_users")
    for page in paginator.paginate(IdentityStoreId=identity_store_id):
        for user in page.get("Users", []):
            name = user.get("Name", {})
            emails = user.get("Emails", [])
            primary_email = ""
            for e in emails:
                if e.get("Primary"):
                    primary_email = e.get("Value", "")
                    break
            if not primary_email and emails:
                primary_email = emails[0].get("Value", "")
            users.append({
                "user_id": user["UserId"],
                "username": user["UserName"],
                "given_name": name.get("GivenName", ""),
                "family_name": name.get("FamilyName", ""),
                "display_name": user.get("DisplayName", ""),
                "email": primary_email,
            })
    return users


def export_all_groups(client, identity_store_id: str) -> list[dict]:
    """Export all groups."""
    groups = []
    paginator = client.get_paginator("list_groups")
    for page in paginator.paginate(IdentityStoreId=identity_store_id):
        for group in page.get("Groups", []):
            groups.append({
                "group_id": group["GroupId"],
                "display_name": group.get("DisplayName", ""),
                "description": group.get("Description", ""),
            })
    return groups


def export_all_memberships(client, identity_store_id: str, group_ids: list[str]) -> list[dict]:
    """Export all group memberships. Returns list of {group_id, user_id}."""
    memberships = []
    for gid in group_ids:
        paginator = client.get_paginator("list_group_memberships")
        for page in paginator.paginate(IdentityStoreId=identity_store_id, GroupId=gid):
            for m in page.get("GroupMemberships", []):
                member = m.get("MemberId", {})
                user_id = member.get("UserId")
                if user_id:
                    memberships.append({
                        "group_id": gid,
                        "user_id": user_id,
                    })
    return memberships


# ---------------------------------------------------------------------------
# AWS Helpers -- Kiro Subscriptions
# ---------------------------------------------------------------------------

# API subscription type -> canonical display name
SUBSCRIPTION_TYPE_TO_TIER = {
    "KIRO_ENTERPRISE_PRO":      "Kiro Pro",
    "KIRO_ENTERPRISE_PRO_PLUS": "Kiro Pro+",
    "KIRO_ENTERPRISE_POWER":    "Kiro Power",
}


def get_instance_arn(session: boto3.Session, region: str | None = None) -> str:
    """Return the SSO instance ARN."""
    sso_admin = session.client("sso-admin", region_name=region)
    resp = sso_admin.list_instances()
    instances = resp.get("Instances", [])
    if not instances:
        raise RuntimeError("No AWS Identity Center instance found.")
    return instances[0]["InstanceArn"]


def get_kiro_app_arn(session: boto3.Session, instance_arn: str, region: str | None = None) -> str | None:
    """Find the Kiro application ARN (KiroProfile-*) from SSO applications."""
    sso_admin = session.client("sso-admin", region_name=region)
    resp = sso_admin.list_applications(InstanceArn=instance_arn)
    for app in resp.get("Applications", []):
        name = app.get("Name", "")
        if name.startswith("KiroProfile"):
            return app["ApplicationArn"]
    return None


def list_kiro_subscriptions(
    credentials,
    instance_arn: str,
    region: str,
) -> list[dict]:
    """
    Call AWSZornControlPlaneService.ListUserSubscriptions to get all Kiro user subscriptions.

    Returns list of {user_id, kiro_tier, status, activation_date}.
    """
    url = f"https://service.user-subscriptions.{region}.amazonaws.com/"
    body = json.dumps({
        "instanceArn": instance_arn,
        "maxResults": 1000,
        "subscriptionRegion": region,
    })
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "AWSZornControlPlaneService.ListUserSubscriptions",
    }

    request = botocore.awsrequest.AWSRequest(
        method="POST", url=url, data=body, headers=headers,
    )
    signer = botocore.auth.SigV4Auth(credentials, "user-subscriptions", region)
    signer.add_auth(request)

    req = urllib.request.Request(
        url, data=body.encode(), headers=dict(request.headers), method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode()
        raise RuntimeError(f"ListUserSubscriptions failed: HTTP {exc.code}: {error_body}") from exc
    data = json.loads(resp.read().decode())

    results = []
    for sub in data.get("subscriptions", []):
        user_id = sub.get("principal", {}).get("user", "")
        sub_type = sub.get("type", {}).get("amazonQ", "")
        tier = SUBSCRIPTION_TYPE_TO_TIER.get(sub_type, sub_type)
        status = sub.get("status", "")
        activation_date = sub.get("activationDate", "")
        results.append({
            "user_id": user_id,
            "kiro_tier": tier,
            "subscription_type": sub_type,
            "status": status,
            "activation_date": activation_date,
        })
    return results


def list_kiro_group_subscriptions(
    credentials,
    app_arn: str,
    region: str,
) -> list[dict]:
    """
    Call AWSZornControlPlaneService.ListApplicationClaims to get group subscriptions.

    Returns list of {group_id, kiro_tier, subscription_type}.
    """
    url = f"https://service.user-subscriptions.{region}.amazonaws.com/"
    body = json.dumps({
        "applicationArn": app_arn,
        "subscriptionRegion": region,
        "maxResults": 100,
    })
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "AWSZornControlPlaneService.ListApplicationClaims",
    }

    request = botocore.awsrequest.AWSRequest(
        method="POST", url=url, data=body, headers=headers,
    )
    signer = botocore.auth.SigV4Auth(credentials, "user-subscriptions", region)
    signer.add_auth(request)

    req = urllib.request.Request(
        url, data=body.encode(), headers=dict(request.headers), method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode()
        raise RuntimeError(f"ListApplicationClaims failed: HTTP {exc.code}: {error_body}") from exc
    data = json.loads(resp.read().decode())

    results = []
    for claim in data.get("claims", []):
        group_id = claim.get("principal", {}).get("group", "")
        if not group_id:
            continue  # skip user claims, we only want groups
        sub_type = claim.get("type", {}).get("amazonQ", "")
        tier = SUBSCRIPTION_TYPE_TO_TIER.get(sub_type, sub_type)
        results.append({
            "group_id": group_id,
            "kiro_tier": tier,
            "subscription_type": sub_type,
        })
    return results


# ---------------------------------------------------------------------------
# AWS Helpers -- Password Reset
# ---------------------------------------------------------------------------

def send_password_reset(user_id: str, credentials, region: str) -> tuple[bool, str]:
    """
    Call SWBUPService.UpdatePassword to send a password reset email.
    Returns (success, error_message).
    """
    url = f"https://identitystore.{region}.amazonaws.com/"
    body = json.dumps({"UserId": user_id, "PasswordMode": "EMAIL"})
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "SWBUPService.UpdatePassword",
    }

    request = botocore.awsrequest.AWSRequest(
        method="POST", url=url, data=body, headers=headers,
    )
    signer = botocore.auth.SigV4Auth(credentials, "userpool", region)
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
        return False, f"HTTP {exc.code}: {error_body}"
    except Exception as exc:
        return False, str(exc)


def bulk_reset_password(
    user_map: dict[str, str],
    credentials,
    region: str,
    workers: int = 5,
) -> tuple[list[str], list[tuple[str, str]]]:
    """
    Send password reset emails in parallel.

    Args:
        user_map: {username: user_id}
        credentials: frozen AWS credentials
        region: AWS region
        workers: number of parallel workers

    Returns:
        (succeeded_usernames, [(failed_username, error), ...])
    """
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []

    def _reset(username: str, user_id: str) -> tuple[str, bool, str]:
        ok, err = send_password_reset(user_id, credentials, region)
        return username, ok, err

    with ThreadPoolExecutor(max_workers=min(workers, len(user_map))) as pool:
        futures = {
            pool.submit(_reset, uname, uid): uname
            for uname, uid in user_map.items()
        }
        for fut in as_completed(futures):
            username, ok, err = fut.result()
            if ok:
                logging.info("Sent password reset: %s", username)
                succeeded.append(username)
            else:
                logging.error("Failed password reset for %s: %s", username, err)
                failed.append((username, err))

    return succeeded, failed


# ---------------------------------------------------------------------------
# CSV Parsing
# ---------------------------------------------------------------------------

def parse_csv(path: Path) -> list[UserRecord]:
    """Parse the CSV file and return a list of UserRecords."""
    records: list[UserRecord] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # header is line 1
            try:
                records.append(UserRecord.from_row(row))
            except ValueError as exc:
                logging.warning("Skipping row %d: %s", i, exc)
    return records


def parse_csv_simple(csv_path: Path) -> list[dict]:
    """Parse CSV returning simple dicts with username + email."""
    users = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            norm = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items()}
            username = norm.get("username") or norm.get("user_name") or norm.get("email") or ""
            email = norm.get("email") or norm.get("email_address") or ""
            if username:
                users.append({"username": username, "email": email})
    return users


def load_report(report_path: Path) -> list[dict]:
    """Read the JSON report from create-users."""
    data = json.loads(report_path.read_text())
    return data.get("created", [])


# ---------------------------------------------------------------------------
# Subcommand: create-users
# ---------------------------------------------------------------------------

def cmd_create_users(args: argparse.Namespace) -> None:
    """Bulk create users from a CSV file."""
    session = boto3.Session(profile_name=args.profile, region_name=args.region)

    # Resolve identity store
    if args.identity_store_id:
        id_store = args.identity_store_id
    else:
        id_store, _ = get_identity_store_id(session, args.region)
    logging.info("Using Identity Store: %s", id_store)

    client = session.client("identitystore", region_name=args.region)

    users = parse_csv(args.csv_file)
    if not users:
        logging.error("No valid user records found in %s", args.csv_file)
        sys.exit(1)

    logging.info("Parsed %d user(s) from %s", len(users), args.csv_file)

    if args.dry_run:
        logging.info("[DRY RUN] Would create the following users:")
        for u in users:
            logging.info("  %s <%s> (%s)", u.display_name, u.email, u.username)
        return

    created: list[dict] = []
    skipped: list[dict] = []
    failed: list[dict] = []
    group_cache: dict[str, str | None] = {}

    for u in users:
        try:
            resp = create_user(client, id_store, u)
            user_id = resp["UserId"]
            logging.info("Created user: %s (UserId: %s)", u.username, user_id)
            created.append({"username": u.username, "email": u.email, "user_id": user_id})

            # Group membership
            for gname in u.group_names:
                gid = resolve_group_id(client, id_store, gname, group_cache)
                if gid:
                    add_user_to_group(client, id_store, gid, user_id)
                    logging.info("  Added to group: %s", gname)
                else:
                    logging.warning("  Group not found: %s", gname)

        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ConflictException":
                logging.warning("Skipped (already exists): %s", u.username)
                skipped.append({"username": u.username, "email": u.email, "reason": "already exists"})
            else:
                logging.error("Failed to create %s: %s", u.username, exc)
                failed.append({"username": u.username, "email": u.email, "error": str(exc)})
        except Exception as exc:
            logging.error("Unexpected error for %s: %s", u.username, exc)
            failed.append({"username": u.username, "email": u.email, "error": str(exc)})

    result = {"created": created, "skipped": skipped, "failed": failed}

    # Summary
    print("\n" + "=" * 60)
    print("CREATE USERS SUMMARY")
    print("=" * 60)
    print(f"  Created : {len(created)}")
    print(f"  Skipped : {len(skipped)}")
    print(f"  Failed  : {len(failed)}")
    print("=" * 60)

    if failed:
        print("\nFailed users:")
        for f_item in failed:
            print(f"  - {f_item['username']} ({f_item['email']}): {f_item['error']}")

    # Write report
    if args.output:
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False))
        logging.info("Report written to %s", args.output)

    # --reset-password: send password reset emails for created users
    if args.reset_password and created:
        print()
        logging.info("Sending password reset emails for %d created user(s)...", len(created))
        credentials = session.get_credentials().get_frozen_credentials()
        user_map = {u["username"]: u["user_id"] for u in created}
        succeeded, reset_failed = bulk_reset_password(
            user_map, credentials, args.region, workers=args.workers,
        )

        print("\n" + "=" * 60)
        print("PASSWORD RESET SUMMARY")
        print("=" * 60)
        print(f"  Sent   : {len(succeeded)}")
        print(f"  Failed : {len(reset_failed)}")
        print("=" * 60)

        if reset_failed:
            print("\nFailed password resets:")
            for uname, err in reset_failed:
                print(f"  - {uname}: {err}")

    if failed:
        sys.exit(1)


# ---------------------------------------------------------------------------
# Subcommand: reset-password
# ---------------------------------------------------------------------------

def cmd_reset_password(args: argparse.Namespace) -> None:
    """Send password reset emails to users."""
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    credentials = session.get_credentials().get_frozen_credentials()

    user_map: dict[str, str] = {}  # {username: user_id}

    if args.report:
        if not args.report.exists():
            logging.error("Report file not found: %s", args.report)
            sys.exit(1)
        users = load_report(args.report)
        if not users:
            logging.error("No created users found in report")
            sys.exit(1)
        if "user_id" not in users[0]:
            logging.error("Report does not contain user_id. Use --csv instead.")
            sys.exit(1)
        user_map = {u["username"]: u["user_id"] for u in users}

    elif args.csv:
        if not args.csv.exists():
            logging.error("CSV file not found: %s", args.csv)
            sys.exit(1)
        csv_users = parse_csv_simple(args.csv)
        if not csv_users:
            logging.error("No users found in CSV")
            sys.exit(1)

        logging.info("Resolving usernames to UserIds...")
        id_store, _ = get_identity_store_id(session, args.region)
        all_users = list_all_users(session, id_store, args.region)
        for u in csv_users:
            uid = all_users.get(u["username"])
            if uid:
                user_map[u["username"]] = uid
            else:
                logging.warning("User '%s' not found in Identity Center, skipping", u["username"])

    if not user_map:
        logging.error("No users to process")
        sys.exit(1)

    logging.info(
        "Will send password reset emails for %d user(s) with %d worker(s)",
        len(user_map), min(args.workers, len(user_map)),
    )

    succeeded, failed = bulk_reset_password(
        user_map, credentials, args.region, workers=args.workers,
    )

    print("\n" + "=" * 60)
    print("PASSWORD RESET SUMMARY")
    print("=" * 60)
    print(f"  Sent   : {len(succeeded)}")
    print(f"  Failed : {len(failed)}")
    print("=" * 60)

    if failed:
        print("\nFailed users:")
        for uname, err in failed:
            print(f"  - {uname}: {err}")
        sys.exit(1)

    logging.info("Done.")


# ---------------------------------------------------------------------------
# Subcommand: enrich
# ---------------------------------------------------------------------------

# Map Kiro plan names from the export CSV to canonical tier names
KIRO_PLAN_TO_TIER = {
    "kiro pro":   "Kiro Pro",
    "kiro pro+":  "Kiro Pro+",
    "kiro power": "Kiro Power",
    "pro":        "Kiro Pro",
    "pro+":       "Kiro Pro+",
    "power":      "Kiro Power",
}


def parse_kiro_subscription_csv(path: Path) -> list[dict]:
    """
    Parse a Kiro Subscriptions List CSV exported from the AWS Console.

    Expected columns: Name, Subscription status, Kiro plan, Plan source, Activation date
    Returns list of {username, kiro_plan, subscription_status}.
    """
    records = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            norm = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items()}
            username = norm.get("name") or norm.get("username") or ""
            kiro_plan = norm.get("kiro_plan") or norm.get("plan") or ""
            status = norm.get("subscription_status") or norm.get("status") or ""
            if username:
                tier = KIRO_PLAN_TO_TIER.get(kiro_plan.lower(), kiro_plan)
                records.append({
                    "username": username,
                    "kiro_plan": kiro_plan,
                    "kiro_tier": tier,
                    "subscription_status": status,
                })
    return records


def cmd_enrich(args: argparse.Namespace) -> None:
    """Enrich a Kiro subscription list with Identity Center user details."""
    if not args.subscription_csv.exists():
        logging.error("Subscription CSV not found: %s", args.subscription_csv)
        sys.exit(1)

    subscriptions = parse_kiro_subscription_csv(args.subscription_csv)
    if not subscriptions:
        logging.error("No users found in subscription CSV")
        sys.exit(1)

    logging.info("Parsed %d user(s) from subscription list", len(subscriptions))

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    id_store, _ = get_identity_store_id(session, args.region)
    logging.info("Using Identity Store: %s", id_store)

    # Fetch all users with full details
    logging.info("Fetching user details from Identity Center...")
    all_users = list_all_users_full(session, id_store, args.region)

    # Optionally fetch group memberships
    client = session.client("identitystore", region_name=args.region)
    include_groups = not args.no_groups

    # Build enriched rows
    enriched = []
    not_found = []
    for sub in subscriptions:
        username = sub["username"]
        user_info = all_users.get(username)
        if not user_info:
            logging.warning("User '%s' not found in Identity Center, skipping", username)
            not_found.append(username)
            continue

        groups = []
        if include_groups:
            logging.debug("Fetching groups for %s...", username)
            groups = list_user_groups(client, id_store, user_info["user_id"])

        enriched.append({
            "UserName": username,
            "GivenName": user_info["given_name"],
            "FamilyName": user_info["family_name"],
            "DisplayName": user_info["display_name"],
            "Email": user_info["email"],
            "Groups": ",".join(groups),
            "KiroTier": sub["kiro_tier"],
        })

    if not enriched:
        logging.error("No users could be enriched")
        sys.exit(1)

    # Write output CSV
    output_path = args.output or Path("enriched_users.csv")
    fieldnames = ["UserName", "GivenName", "FamilyName", "DisplayName", "Email", "Groups", "KiroTier"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched)

    print("\n" + "=" * 60)
    print("ENRICH SUMMARY")
    print("=" * 60)
    print(f"  Enriched  : {len(enriched)}")
    print(f"  Not found : {len(not_found)}")
    print(f"  Output    : {output_path}")
    print("=" * 60)

    if not_found:
        print("\nUsers not found in Identity Center:")
        for u in not_found:
            print(f"  - {u}")

    logging.info("Done.")


# ---------------------------------------------------------------------------
# Subcommand: export-subscriptions
# ---------------------------------------------------------------------------

def cmd_export_subscriptions(args: argparse.Namespace) -> None:
    """Export Kiro subscriptions (users and groups) with enriched details."""
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    credentials = session.get_credentials().get_frozen_credentials()

    # Get instance ARN and Identity Store ID
    instance_arn = get_instance_arn(session, args.region)
    id_store, _ = get_identity_store_id(session, args.region)
    logging.info("Instance ARN: %s", instance_arn)
    logging.info("Identity Store: %s", id_store)

    # Fetch user subscriptions
    logging.info("Fetching Kiro user subscriptions...")
    subscriptions = list_kiro_subscriptions(credentials, instance_arn, args.region)
    logging.info("  Found %d user subscription(s)", len(subscriptions))

    # Fetch group subscriptions
    group_subscriptions = []
    app_arn = get_kiro_app_arn(session, instance_arn, args.region)
    if app_arn:
        logging.info("Fetching Kiro group subscriptions...")
        group_subscriptions = list_kiro_group_subscriptions(credentials, app_arn, args.region)
        logging.info("  Found %d group subscription(s)", len(group_subscriptions))
    else:
        logging.warning("Kiro application not found -- skipping group subscriptions")

    # Fetch user details to map user_id -> username/email
    logging.info("Fetching user details from Identity Center...")
    all_users = list_all_users_full(session, id_store, args.region)
    user_id_to_info = {v["user_id"]: {**v, "username": k} for k, v in all_users.items()}

    # Fetch group details for group subscriptions
    client = session.client("identitystore", region_name=args.region)
    group_id_to_name: dict[str, str] = {}
    for gs in group_subscriptions:
        gid = gs["group_id"]
        if gid not in group_id_to_name:
            try:
                resp = client.describe_group(IdentityStoreId=id_store, GroupId=gid)
                group_id_to_name[gid] = resp.get("DisplayName", gid)
            except ClientError:
                group_id_to_name[gid] = gid

    # Build enriched user rows
    user_rows = []
    for sub in subscriptions:
        user_info = user_id_to_info.get(sub["user_id"], {})
        row: dict = {
            "UserName": user_info.get("username", sub["user_id"]),
            "GivenName": user_info.get("given_name", ""),
            "FamilyName": user_info.get("family_name", ""),
            "DisplayName": user_info.get("display_name", ""),
            "Email": user_info.get("email", ""),
            "KiroTier": sub["kiro_tier"],
            "Status": sub["status"],
        }
        # Optionally fetch Identity Center group memberships
        if not args.no_groups:
            uid = user_info.get("user_id", sub["user_id"])
            groups = list_user_groups(client, id_store, uid)
            row["Groups"] = ",".join(groups)
        else:
            row["Groups"] = ""
        user_rows.append(row)

    # Build group subscription rows
    group_rows = []
    for gs in group_subscriptions:
        group_rows.append({
            "GroupName": group_id_to_name.get(gs["group_id"], gs["group_id"]),
            "KiroTier": gs["kiro_tier"],
        })

    # Write user subscriptions CSV
    output_path = args.output or Path("kiro-subscriptions.csv")
    fieldnames = ["UserName", "GivenName", "FamilyName", "DisplayName", "Email", "Groups", "KiroTier", "Status"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(user_rows)

    # Write group subscriptions CSV
    group_output = output_path.parent / (output_path.stem + "-groups" + output_path.suffix)
    group_fieldnames = ["GroupName", "KiroTier"]
    with open(group_output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=group_fieldnames)
        writer.writeheader()
        writer.writerows(group_rows)

    # Summary
    tier_counts: dict[str, int] = {}
    for row in user_rows:
        tier_counts[row["KiroTier"]] = tier_counts.get(row["KiroTier"], 0) + 1

    print("\n" + "=" * 60)
    print("EXPORT SUBSCRIPTIONS SUMMARY")
    print("=" * 60)
    print(f"  User subscriptions  : {len(user_rows)}")
    for tier, count in sorted(tier_counts.items()):
        print(f"    {tier:12s} : {count}")
    print(f"  Group subscriptions : {len(group_rows)}")
    for gr in group_rows:
        print(f"    {gr['GroupName']:12s} : {gr['KiroTier']}")
    print(f"  Output (users)      : {output_path}")
    print(f"  Output (groups)     : {group_output}")
    print("=" * 60)

    logging.info("Done.")


# ---------------------------------------------------------------------------
# Subcommand: export-store
# ---------------------------------------------------------------------------

def cmd_export_store(args: argparse.Namespace) -> None:
    """Export all users, groups, and memberships from an Identity Store."""
    session = boto3.Session(profile_name=args.profile, region_name=args.region)

    if args.identity_store_id:
        id_store = args.identity_store_id
    else:
        id_store, _ = get_identity_store_id(session, args.region)

    logging.info("Exporting Identity Store: %s", id_store)
    client = session.client("identitystore", region_name=args.region)

    # Export users
    logging.info("Exporting users...")
    users = export_all_users(client, id_store)
    logging.info("  Found %d user(s)", len(users))

    # Export groups
    logging.info("Exporting groups...")
    groups = export_all_groups(client, id_store)
    logging.info("  Found %d group(s)", len(groups))

    # Export memberships
    logging.info("Exporting group memberships...")
    group_ids = [g["group_id"] for g in groups]
    memberships = export_all_memberships(client, id_store, group_ids)
    logging.info("  Found %d membership(s)", len(memberships))

    # Build username/group-name readable mappings for the memberships
    user_id_to_name = {u["user_id"]: u["username"] for u in users}
    group_id_to_name = {g["group_id"]: g["display_name"] for g in groups}
    readable_memberships = []
    for m in memberships:
        readable_memberships.append({
            "group_id": m["group_id"],
            "group_name": group_id_to_name.get(m["group_id"], ""),
            "user_id": m["user_id"],
            "username": user_id_to_name.get(m["user_id"], ""),
        })

    export_data = {
        "identity_store_id": id_store,
        "users": users,
        "groups": groups,
        "memberships": readable_memberships,
    }

    output_path = args.output or Path("identity-store-export.json")
    output_path.write_text(json.dumps(export_data, indent=2, ensure_ascii=False))

    print("\n" + "=" * 60)
    print("EXPORT SUMMARY")
    print("=" * 60)
    print(f"  Identity Store : {id_store}")
    print(f"  Users          : {len(users)}")
    print(f"  Groups         : {len(groups)}")
    print(f"  Memberships    : {len(memberships)}")
    print(f"  Output         : {output_path}")
    print("=" * 60)

    logging.info("Done.")


# ---------------------------------------------------------------------------
# Subcommand: import-store
# ---------------------------------------------------------------------------

def cmd_import_store(args: argparse.Namespace) -> None:
    """Import users, groups, and memberships into an Identity Store."""
    import_path = args.import_file
    if not import_path.exists():
        logging.error("Import file not found: %s", import_path)
        sys.exit(1)

    data = json.loads(import_path.read_text())
    src_users = data.get("users", [])
    src_groups = data.get("groups", [])
    src_memberships = data.get("memberships", [])

    logging.info(
        "Loaded export: %d users, %d groups, %d memberships",
        len(src_users), len(src_groups), len(src_memberships),
    )

    session = boto3.Session(profile_name=args.profile, region_name=args.region)

    if args.identity_store_id:
        target_id_store = args.identity_store_id
    else:
        target_id_store, _ = get_identity_store_id(session, args.region)

    source_id_store = data.get("identity_store_id", "")
    if target_id_store == source_id_store and not args.force:
        logging.error(
            "Target Identity Store (%s) is the same as source. "
            "Use --force to import into the same store.",
            target_id_store,
        )
        sys.exit(1)

    logging.info("Target Identity Store: %s", target_id_store)
    client = session.client("identitystore", region_name=args.region)

    if args.dry_run:
        logging.info("[DRY RUN] Would import:")
        logging.info("  %d user(s)", len(src_users))
        for u in src_users:
            logging.info("    %s <%s>", u["username"], u["email"])
        logging.info("  %d group(s)", len(src_groups))
        for g in src_groups:
            logging.info("    %s", g["display_name"])
        logging.info("  %d membership(s)", len(src_memberships))
        return

    # ---- Phase 1: Create groups -------------------------------------------
    logging.info("Phase 1: Creating groups...")
    # old_group_id -> new_group_id
    group_id_map: dict[str, str] = {}
    groups_created = 0
    groups_skipped = 0

    for g in src_groups:
        try:
            create_params: dict = {
                "IdentityStoreId": target_id_store,
                "DisplayName": g["display_name"],
            }
            if g.get("description"):
                create_params["Description"] = g["description"]
            resp = client.create_group(**create_params)
            new_gid = resp["GroupId"]
            group_id_map[g["group_id"]] = new_gid
            groups_created += 1
            logging.info("  Created group: %s (GroupId: %s)", g["display_name"], new_gid)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConflictException":
                # Group exists -- resolve its ID
                resolved = resolve_group_id(client, target_id_store, g["display_name"], {})
                if resolved:
                    group_id_map[g["group_id"]] = resolved
                groups_skipped += 1
                logging.warning("  Skipped (exists): %s", g["display_name"])
            else:
                logging.error("  Failed to create group %s: %s", g["display_name"], exc)

    # ---- Phase 2: Create users --------------------------------------------
    logging.info("Phase 2: Creating users...")
    # old_user_id -> new_user_id
    user_id_map: dict[str, str] = {}
    users_created = 0
    users_skipped = 0
    users_failed = 0
    target_users_cache: dict[str, str] | None = None  # lazy-loaded on first conflict

    for u in src_users:
        try:
            params: dict = {
                "IdentityStoreId": target_id_store,
                "UserName": u["username"],
                "Name": {
                    "GivenName": u["given_name"] or u["username"],
                    "FamilyName": u["family_name"] or u["username"],
                },
                "DisplayName": u["display_name"] or u["username"],
            }
            if u["email"]:
                params["Emails"] = [{"Value": u["email"], "Type": "Work", "Primary": True}]

            resp = client.create_user(**params)
            new_uid = resp["UserId"]
            user_id_map[u["user_id"]] = new_uid
            users_created += 1
            logging.info("  Created user: %s (UserId: %s)", u["username"], new_uid)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConflictException":
                # User exists -- resolve their ID (lazy-load once)
                if target_users_cache is None:
                    target_users_cache = list_all_users(session, target_id_store, args.region)
                resolved_uid = target_users_cache.get(u["username"])
                if resolved_uid:
                    user_id_map[u["user_id"]] = resolved_uid
                users_skipped += 1
                logging.warning("  Skipped (exists): %s", u["username"])
            else:
                users_failed += 1
                logging.error("  Failed to create user %s: %s", u["username"], exc)

    # ---- Phase 3: Create memberships --------------------------------------
    logging.info("Phase 3: Creating group memberships...")
    memberships_created = 0
    memberships_skipped = 0
    memberships_failed = 0

    for m in src_memberships:
        new_gid = group_id_map.get(m["group_id"])
        new_uid = user_id_map.get(m["user_id"])
        label = f"{m.get('username', m['user_id'])} -> {m.get('group_name', m['group_id'])}"

        if not new_gid:
            logging.warning("  Skipped membership (group not mapped): %s", label)
            memberships_failed += 1
            continue
        if not new_uid:
            logging.warning("  Skipped membership (user not mapped): %s", label)
            memberships_failed += 1
            continue

        try:
            client.create_group_membership(
                IdentityStoreId=target_id_store,
                GroupId=new_gid,
                MemberId={"UserId": new_uid},
            )
            memberships_created += 1
            logging.info("  Added: %s", label)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConflictException":
                memberships_skipped += 1
                logging.warning("  Skipped (exists): %s", label)
            else:
                memberships_failed += 1
                logging.error("  Failed: %s -- %s", label, exc)

    # ---- Summary ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    print(f"  Target Store   : {target_id_store}")
    print(f"  Groups created : {groups_created}  skipped: {groups_skipped}")
    print(f"  Users created  : {users_created}  skipped: {users_skipped}  failed: {users_failed}")
    print(f"  Memberships    : {memberships_created}  skipped: {memberships_skipped}  failed: {memberships_failed}")
    print("=" * 60)

    if users_failed or memberships_failed:
        sys.exit(1)

    logging.info("Done.")

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="idc_manager",
        description="AWS Identity Center Manager -- bulk user operations.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # -- create-users -------------------------------------------------------
    p_create = subparsers.add_parser(
        "create-users",
        help="Bulk create users from a CSV file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
CSV Columns (flexible naming, case-insensitive):
  UserName       - unique login name (falls back to Email)
  GivenName      - first name
  FamilyName     - last name
  DisplayName    - display name (auto-generated if omitted)
  Email          - primary email address (required)
  Groups         - comma-separated group names (optional)

Examples:
  idc_manager.py create-users users.csv
  idc_manager.py create-users users.csv --dry-run
  idc_manager.py create-users users.csv --reset-password
  idc_manager.py create-users users.csv -r us-east-1 -o report.json --reset-password
""",
    )
    p_create.add_argument("csv_file", type=Path, help="Path to the CSV file")
    p_create.add_argument("--identity-store-id", "-i", help="Identity Store ID (auto-detected if omitted)")
    p_create.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    p_create.add_argument("--profile", "-p", help="AWS CLI named profile")
    p_create.add_argument("--dry-run", "-n", action="store_true", help="Validate CSV without creating users")
    p_create.add_argument("--output", "-o", type=Path, help="Write JSON report to file")
    p_create.add_argument(
        "--reset-password", action="store_true",
        help="Send password reset emails to all created users after import",
    )
    p_create.add_argument("--workers", "-w", type=int, default=5, help="Parallel workers for password reset (default: 5)")
    p_create.set_defaults(func=cmd_create_users)

    # -- reset-password -----------------------------------------------------
    p_reset = subparsers.add_parser(
        "reset-password",
        help="Send password reset emails to users",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  idc_manager.py reset-password --csv users.csv
  idc_manager.py reset-password --report report.json
  idc_manager.py reset-password --csv users.csv -r us-east-1 --workers 10
""",
    )
    reset_input = p_reset.add_mutually_exclusive_group(required=True)
    reset_input.add_argument("--csv", type=Path, help="CSV file with users")
    reset_input.add_argument("--report", type=Path, help="JSON report from create-users")
    p_reset.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    p_reset.add_argument("--profile", "-p", help="AWS CLI named profile")
    p_reset.add_argument("--workers", "-w", type=int, default=5, help="Parallel workers (default: 5)")
    p_reset.set_defaults(func=cmd_reset_password)

    # -- enrich -------------------------------------------------------------
    p_enrich = subparsers.add_parser(
        "enrich",
        help="Enrich a Kiro subscription list with Identity Center user details",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Reads a Kiro Subscriptions List CSV (exported from the AWS Console) and
queries Identity Center to fill in user details (name, email, groups).
Outputs a full CSV compatible with create-users and kiro_subscribe.py.

Expected input columns: Name, Subscription status, Kiro plan, ...

Examples:
  idc_manager.py enrich "Kiro Subscriptions List.csv"
  idc_manager.py enrich "Kiro Subscriptions List.csv" -o users.csv
  idc_manager.py enrich "Kiro Subscriptions List.csv" --no-groups
""",
    )
    p_enrich.add_argument("subscription_csv", type=Path, help="Kiro subscription list CSV file")
    p_enrich.add_argument("--output", "-o", type=Path, help="Output CSV path (default: enriched_users.csv)")
    p_enrich.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    p_enrich.add_argument("--profile", "-p", help="AWS CLI named profile")
    p_enrich.add_argument("--no-groups", action="store_true", help="Skip fetching group memberships (faster)")
    p_enrich.set_defaults(func=cmd_enrich)

    # -- export-subscriptions -----------------------------------------------
    p_expsub = subparsers.add_parser(
        "export-subscriptions",
        help="Export Kiro subscriptions with enriched user details",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Fetches all Kiro subscriptions via the ListUserSubscriptions API, then
enriches each entry with user details (name, email, groups) from
Identity Center. Outputs a CSV compatible with kiro_subscribe.py.

This replaces the manual "Download CSV" step from the Kiro console.

Examples:
  idc_manager.py export-subscriptions
  idc_manager.py export-subscriptions -o kiro-subscriptions.csv
  idc_manager.py export-subscriptions --no-groups --profile source-account
""",
    )
    p_expsub.add_argument("--output", "-o", type=Path, help="Output CSV path (default: kiro-subscriptions.csv)")
    p_expsub.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    p_expsub.add_argument("--profile", "-p", help="AWS CLI named profile")
    p_expsub.add_argument("--no-groups", action="store_true", help="Skip fetching group memberships (faster)")
    p_expsub.set_defaults(func=cmd_export_subscriptions)

    # -- export-store -------------------------------------------------------
    p_export = subparsers.add_parser(
        "export-store",
        help="Export all users, groups, and memberships from an Identity Store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Exports the entire Identity Store to a JSON file containing:
  - All users (username, name, email)
  - All groups (display name, description)
  - All group memberships (user <-> group mapping)

The output file can be used with import-store to replicate the store.

Examples:
  idc_manager.py export-store
  idc_manager.py export-store -o backup.json
  idc_manager.py export-store -i d-1234567890 -r us-east-1
""",
    )
    p_export.add_argument("--identity-store-id", "-i", help="Identity Store ID (auto-detected if omitted)")
    p_export.add_argument("--output", "-o", type=Path, help="Output JSON path (default: identity-store-export.json)")
    p_export.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    p_export.add_argument("--profile", "-p", help="AWS CLI named profile")
    p_export.set_defaults(func=cmd_export_store)

    # -- import-store -------------------------------------------------------
    p_import = subparsers.add_parser(
        "import-store",
        help="Import users, groups, and memberships into an Identity Store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Imports users, groups, and memberships from a JSON file (produced by
export-store) into a target Identity Store.

The import is idempotent: existing users/groups are skipped and their
IDs are resolved for membership creation.

Import order: groups -> users -> memberships.

Examples:
  idc_manager.py import-store backup.json -i d-9876543210
  idc_manager.py import-store backup.json -i d-9876543210 --dry-run
  idc_manager.py import-store backup.json -r us-west-2 --profile target-account
""",
    )
    p_import.add_argument("import_file", type=Path, help="JSON file from export-store")
    p_import.add_argument("--identity-store-id", "-i", help="Target Identity Store ID (auto-detected if omitted)")
    p_import.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    p_import.add_argument("--profile", "-p", help="AWS CLI named profile")
    p_import.add_argument("--dry-run", "-n", action="store_true", help="Show what would be imported without making changes")
    p_import.add_argument("--force", action="store_true", help="Allow importing into the same Identity Store as source")
    p_import.set_defaults(func=cmd_import_store)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args.func(args)


if __name__ == "__main__":
    main()
