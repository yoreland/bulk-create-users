#!/usr/bin/env python3
"""
Bulk Unsubscribe Users from Kiro Tiers

Calls the AmazonQDeveloperService.DeleteAssignment API to remove
AWS Identity Center users from Kiro plans.

Note: Deletion enters "pending removal" state and becomes effective at the end of the billing month.

Usage:
  python kiro_unsubscribe.py --csv users.csv --region us-east-1
  python kiro_unsubscribe.py --username user1 --region us-east-1
"""

import argparse
import csv
import json
import logging
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import botocore.auth
import botocore.awsrequest


# ---------------------------------------------------------------------------
# AWS API
# ---------------------------------------------------------------------------

def delete_assignment(
    principal_id: str,
    credentials,
    region: str,
    principal_type: str = "USER",
) -> tuple[bool, str]:
    """
    Call AmazonQDeveloperService.DeleteAssignment to unsubscribe a user or group.

    Note: Deletion enters "pending removal" state, effective at end of month.
    Returns (success, error_message).
    """
    url = f"https://codewhisperer.{region}.amazonaws.com/"
    body = json.dumps({
        "principalId": principal_id,
        "principalType": principal_type,
    })
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": "AmazonQDeveloperService.DeleteAssignment",
    }

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
        return False, f"HTTP {exc.code}: {error_body}"
    except Exception as exc:
        return False, str(exc)


def get_instance_arn(session: boto3.Session, region: str | None = None) -> str:
    """Get Identity Center instance ARN."""
    sso_admin = session.client("sso-admin", region_name=region)
    resp = sso_admin.list_instances()
    instances = resp.get("Instances", [])
    if not instances:
        raise RuntimeError("No AWS Identity Center instance found.")
    return instances[0]["InstanceArn"]


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


def list_user_subscriptions(
    credentials,
    instance_arn: str,
    region: str,
) -> set[str]:
    """
    Call AWSZornControlPlaneService.ListUserSubscriptions to get subscribed user IDs.
    Returns set of user IDs who have Kiro subscriptions.
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
        data = json.loads(resp.read().decode())
        
        result = set()
        for sub in data.get("subscriptions", []):
            user_id = sub.get("principal", {}).get("user", "")
            if user_id:
                result.add(user_id)
        return result
    except Exception as exc:
        logging.warning("Failed to list subscriptions: %s", exc)
        return set()


# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------

def get_users_from_csv(csv_path: Path) -> list[str]:
    """Load usernames from CSV."""
    usernames = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            norm = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items()}
            username = norm.get("username") or norm.get("user_name") or norm.get("email") or ""
            if username:
                usernames.append(username)
    return usernames


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove Kiro subscriptions from AWS Identity Center users.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  %(prog)s --csv users.csv --region us-east-1
  %(prog)s --username user1 --region us-east-1
  %(prog)s --csv users.csv --region us-east-1 --dry-run  # Preview only

Note: Deletion enters "pending removal" state, effective at end of billing month.
""",
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--csv", type=Path, help="CSV file with usernames")
    input_group.add_argument("--username", "-u", help="Single username to unsubscribe")

    parser.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--profile", "-p", help="AWS CLI named profile")
    parser.add_argument("--workers", "-w", type=int, default=5, help="Number of parallel workers (default: 5)")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Preview without making changes")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    credentials = session.get_credentials().get_frozen_credentials()

    # Get usernames
    if args.username:
        usernames = [args.username]
    else:
        if not args.csv.exists():
            logging.error("CSV file not found: %s", args.csv)
            sys.exit(1)
        usernames = get_users_from_csv(args.csv)

    if not usernames:
        logging.error("No usernames to process")
        sys.exit(1)

    # Resolve usernames to user IDs
    id_store = get_identity_store_id(session, args.region)
    all_users = list_all_users(session, id_store, args.region)
    
    user_ids = []
    for username in usernames:
        uid = all_users.get(username)
        if uid:
            user_ids.append((username, uid))
        else:
            logging.warning("User '%s' not found in Identity Center, skipping", username)

    if not user_ids:
        logging.error("No valid users to process")
        sys.exit(1)

    # Get current subscriptions to filter users who actually have subscriptions
    instance_arn = get_instance_arn(session, args.region)
    subscribed_user_ids = list_user_subscriptions(credentials, instance_arn, args.region)

    # Filter users who have subscriptions
    to_delete = [(u, uid) for u, uid in user_ids if uid in subscribed_user_ids]

    if not to_delete:
        logging.info("No users with Kiro subscriptions to remove")
        for username, _ in user_ids:
            logging.info("  %s: No subscription", username)
        sys.exit(0)

    # Show plan
    logging.info("Will remove %d subscription(s):", len(to_delete))
    for username, _ in to_delete:
        logging.info("  %s", username)

    skipped = [(u, uid) for u, uid in user_ids if uid not in subscribed_user_ids]
    if skipped:
        logging.info("Skipping %d user(s) without subscriptions:", len(skipped))
        for username, _ in skipped:
            logging.info("  %s", username)

    if args.dry_run:
        logging.info("[DRY RUN] Would delete %d subscription(s)", len(to_delete))
        sys.exit(0)

    # Delete subscriptions
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []

    def _unsubscribe(item: tuple) -> tuple[str, bool, str]:
        username, uid = item
        ok, err = delete_assignment(uid, credentials, args.region)
        return username, ok, err

    with ThreadPoolExecutor(max_workers=min(args.workers, len(to_delete))) as pool:
        futures = {pool.submit(_unsubscribe, item): item[0] for item in to_delete}
        for fut in as_completed(futures):
            username, ok, err = fut.result()
            if ok:
                logging.info("Unsubscribed: %s", username)
                succeeded.append(username)
            else:
                logging.error("Failed for %s: %s", username, err)
                failed.append((username, err))

    # Summary
    print("\n" + "=" * 60)
    print("KIRO UNSUBSCRIBE SUMMARY")
    print("=" * 60)
    print(f"  Removed : {len(succeeded)}")
    print(f"  Failed  : {len(failed)}")
    print("=" * 60)

    if failed:
        print("\nFailed users:")
        for uname, err in failed:
            print(f"  - {uname}: {err}")
        sys.exit(1)

    logging.info("Done. Note: Subscriptions enter 'pending removal' state, effective at end of billing month.")


if __name__ == "__main__":
    main()
