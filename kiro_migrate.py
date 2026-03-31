#!/usr/bin/env python3
"""
Kiro Migration Tool

Fully automated migration of Kiro subscriptions from one AWS account to another.
Combines all 5 migration steps into a single command:

  1a. Export Identity Store (users, groups, memberships) from source
  1b. Export Kiro subscriptions (with user details) from source
  2a. Import Identity Store into target
  2b. Send password reset emails in target
  3.  Re-create Kiro subscriptions in target

Usage:
  python kiro_migrate.py \\
    --source-profile source-account \\
    --target-profile target-account \\
    --region us-east-1

  python kiro_migrate.py \\
    --source-profile source-account \\
    --target-profile target-account \\
    --region us-east-1 \\
    --dry-run
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3

from idc_manager import (
    bulk_reset_password,
    export_all_groups,
    export_all_memberships,
    export_all_users,
    get_identity_store_id,
    get_instance_arn,
    get_kiro_app_arn,
    list_all_users,
    list_kiro_group_subscriptions,
    list_kiro_subscriptions,
    resolve_group_id,
    SUBSCRIPTION_TYPE_TO_TIER,
)
from kiro_subscribe import (
    create_assignment,
    resolve_tier,
)
from botocore.exceptions import ClientError


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate Kiro subscriptions from one AWS account to another.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
This tool automates the full migration workflow:

  Source account:
    1a. Export Identity Store (users, groups, memberships)
    1b. Export Kiro subscriptions with user details

  Target account:
    2a. Import Identity Store (groups -> users -> memberships)
    2b. Send password reset emails
    3.  Re-create Kiro subscriptions

All steps are idempotent -- safe to re-run if interrupted.

Examples:
  %(prog)s --source-profile acct-a --target-profile acct-b --region us-east-1
  %(prog)s --source-profile acct-a --target-profile acct-b --region us-east-1 --dry-run
  %(prog)s --source-profile acct-a --target-profile acct-b --region us-east-1 --skip-reset-password
""",
    )
    parser.add_argument("--source-profile", required=True, help="AWS CLI profile for the source account")
    parser.add_argument("--target-profile", required=True, help="AWS CLI profile for the target account")
    parser.add_argument("--region", "-r", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Preview all steps without making changes")
    parser.add_argument("--skip-reset-password", action="store_true", help="Skip sending password reset emails")
    parser.add_argument("--skip-subscriptions", action="store_true", help="Skip re-creating Kiro subscriptions")
    parser.add_argument("--workers", "-w", type=int, default=5, help="Parallel workers (default: 5)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    source_session = boto3.Session(profile_name=args.source_profile, region_name=args.region)
    target_session = boto3.Session(profile_name=args.target_profile, region_name=args.region)

    region = args.region

    # ==================================================================
    # Step 1a: Export Identity Store from source
    # ==================================================================
    print("\n" + "=" * 60)
    print("STEP 1a: Export Identity Store (source)")
    print("=" * 60)

    source_id_store, _ = get_identity_store_id(source_session, region)
    source_client = source_session.client("identitystore", region_name=region)
    logging.info("Source Identity Store: %s", source_id_store)

    logging.info("Exporting users...")
    src_users = export_all_users(source_client, source_id_store)
    logging.info("  Found %d user(s)", len(src_users))

    logging.info("Exporting groups...")
    src_groups = export_all_groups(source_client, source_id_store)
    logging.info("  Found %d group(s)", len(src_groups))

    logging.info("Exporting group memberships...")
    group_ids = [g["group_id"] for g in src_groups]
    src_memberships = export_all_memberships(source_client, source_id_store, group_ids)
    logging.info("  Found %d membership(s)", len(src_memberships))

    # Build readable mappings
    user_id_to_name = {u["user_id"]: u["username"] for u in src_users}
    group_id_to_name = {g["group_id"]: g["display_name"] for g in src_groups}
    for m in src_memberships:
        m["username"] = user_id_to_name.get(m["user_id"], "")
        m["group_name"] = group_id_to_name.get(m["group_id"], "")

    # ==================================================================
    # Step 1b: Export Kiro subscriptions from source
    # ==================================================================
    print("\n" + "=" * 60)
    print("STEP 1b: Export Kiro Subscriptions (source)")
    print("=" * 60)

    source_credentials = source_session.get_credentials().get_frozen_credentials()
    source_instance_arn = get_instance_arn(source_session, region)
    logging.info("Source Instance ARN: %s", source_instance_arn)

    logging.info("Fetching Kiro user subscriptions...")
    subscriptions = list_kiro_subscriptions(source_credentials, source_instance_arn, region)
    logging.info("  Found %d user subscription(s)", len(subscriptions))

    # Fetch group subscriptions
    group_subscriptions: list[dict] = []
    source_app_arn = get_kiro_app_arn(source_session, source_instance_arn, region)
    if source_app_arn:
        logging.info("Fetching Kiro group subscriptions...")
        group_subscriptions = list_kiro_group_subscriptions(source_credentials, source_app_arn, region)
        logging.info("  Found %d group subscription(s)", len(group_subscriptions))
    else:
        logging.warning("Kiro application not found -- skipping group subscriptions")

    # Map user_id -> subscription info
    sub_by_user_id = {s["user_id"]: s for s in subscriptions}

    # Map group_id -> group subscription
    group_sub_by_id = {gs["group_id"]: gs for gs in group_subscriptions}

    # Show tier summary
    tier_counts: dict[str, int] = {}
    for s in subscriptions:
        tier_counts[s["kiro_tier"]] = tier_counts.get(s["kiro_tier"], 0) + 1
    for tier, count in sorted(tier_counts.items()):
        logging.info("  %s: %d user(s)", tier, count)
    for gs in group_subscriptions:
        gname = group_id_to_name.get(gs["group_id"], gs["group_id"])
        logging.info("  Group '%s': %s", gname, gs["kiro_tier"])

    if args.dry_run:
        print("\n" + "=" * 60)
        print("[DRY RUN] Would migrate:")
        print("=" * 60)
        print(f"  Users               : {len(src_users)}")
        print(f"  Groups              : {len(src_groups)}")
        print(f"  Memberships         : {len(src_memberships)}")
        print(f"  User subscriptions  : {len(subscriptions)}")
        print(f"  Group subscriptions : {len(group_subscriptions)}")
        print()
        print("Users:")
        for u in src_users:
            sub = sub_by_user_id.get(u["user_id"])
            tier_info = f" [{sub['kiro_tier']}]" if sub else ""
            print(f"  {u['username']} <{u['email']}>{tier_info}")
        print()
        print("Groups:")
        for g in src_groups:
            gs = group_sub_by_id.get(g["group_id"])
            tier_info = f" [{gs['kiro_tier']}]" if gs else ""
            print(f"  {g['display_name']}{tier_info}")
        print("=" * 60)
        return

    # ==================================================================
    # Step 2a: Import Identity Store into target
    # ==================================================================
    print("\n" + "=" * 60)
    print("STEP 2a: Import Identity Store (target)")
    print("=" * 60)

    target_id_store, _ = get_identity_store_id(target_session, region)
    target_client = target_session.client("identitystore", region_name=region)
    logging.info("Target Identity Store: %s", target_id_store)

    if target_id_store == source_id_store:
        logging.error("Source and target Identity Stores are the same (%s). Aborting.", target_id_store)
        sys.exit(1)

    # Phase 1: Create groups
    logging.info("Creating groups...")
    group_id_map: dict[str, str] = {}  # old -> new
    groups_created = 0
    groups_skipped = 0

    for g in src_groups:
        try:
            params: dict = {
                "IdentityStoreId": target_id_store,
                "DisplayName": g["display_name"],
            }
            if g.get("description"):
                params["Description"] = g["description"]
            resp = target_client.create_group(**params)
            group_id_map[g["group_id"]] = resp["GroupId"]
            groups_created += 1
            logging.info("  Created group: %s", g["display_name"])
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConflictException":
                resolved = resolve_group_id(target_client, target_id_store, g["display_name"], {})
                if resolved:
                    group_id_map[g["group_id"]] = resolved
                groups_skipped += 1
                logging.info("  Skipped (exists): %s", g["display_name"])
            else:
                logging.error("  Failed: %s -- %s", g["display_name"], exc)

    logging.info("  Groups -- created: %d, skipped: %d", groups_created, groups_skipped)

    # Phase 2: Create users
    logging.info("Creating users...")
    user_id_map: dict[str, str] = {}  # old -> new
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

            resp = target_client.create_user(**params)
            user_id_map[u["user_id"]] = resp["UserId"]
            users_created += 1
            logging.info("  Created user: %s", u["username"])
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConflictException":
                if target_users_cache is None:
                    target_users_cache = list_all_users(target_session, target_id_store, region)
                uid = target_users_cache.get(u["username"])
                if uid:
                    user_id_map[u["user_id"]] = uid
                users_skipped += 1
                logging.info("  Skipped (exists): %s", u["username"])
            else:
                users_failed += 1
                logging.error("  Failed: %s -- %s", u["username"], exc)

    logging.info("  Users -- created: %d, skipped: %d, failed: %d", users_created, users_skipped, users_failed)

    # Phase 3: Create memberships
    logging.info("Creating group memberships...")
    memberships_created = 0
    memberships_skipped = 0
    memberships_failed = 0

    for m in src_memberships:
        new_gid = group_id_map.get(m["group_id"])
        new_uid = user_id_map.get(m["user_id"])
        label = f"{m.get('username', '?')} -> {m.get('group_name', '?')}"

        if not new_gid or not new_uid:
            memberships_failed += 1
            logging.warning("  Skipped (unmapped): %s", label)
            continue

        try:
            target_client.create_group_membership(
                IdentityStoreId=target_id_store,
                GroupId=new_gid,
                MemberId={"UserId": new_uid},
            )
            memberships_created += 1
            logging.info("  Added: %s", label)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConflictException":
                memberships_skipped += 1
                logging.info("  Skipped (exists): %s", label)
            else:
                memberships_failed += 1
                logging.error("  Failed: %s -- %s", label, exc)

    logging.info(
        "  Memberships -- created: %d, skipped: %d, failed: %d",
        memberships_created, memberships_skipped, memberships_failed,
    )

    # ==================================================================
    # Step 2b: Send password reset emails
    # ==================================================================
    reset_sent = 0
    reset_failed_count = 0

    if args.skip_reset_password:
        logging.info("Skipping password reset emails (--skip-reset-password)")
    else:
        print("\n" + "=" * 60)
        print("STEP 2b: Send Password Reset Emails (target)")
        print("=" * 60)

        target_credentials = target_session.get_credentials().get_frozen_credentials()
        # Only reset passwords for users that were actually created (not skipped)
        # But to be safe, reset for all mapped users
        reset_map = {
            user_id_to_name.get(old_uid, old_uid): new_uid
            for old_uid, new_uid in user_id_map.items()
        }

        if reset_map:
            succeeded, failed = bulk_reset_password(
                reset_map, target_credentials, region, workers=args.workers,
            )
            reset_sent = len(succeeded)
            reset_failed_count = len(failed)
        else:
            logging.warning("No users to send password reset emails to")

    # ==================================================================
    # Step 3: Re-create Kiro subscriptions (users + groups)
    # ==================================================================
    subs_created = 0
    subs_skipped = 0
    subs_failed_count = 0
    group_subs_created = 0
    group_subs_skipped = 0
    group_subs_failed_count = 0

    if args.skip_subscriptions:
        logging.info("Skipping Kiro subscriptions (--skip-subscriptions)")
    else:
        target_credentials = target_session.get_credentials().get_frozen_credentials()

        # 3a. User subscriptions
        if not subscriptions:
            logging.info("No Kiro user subscriptions to migrate")
        else:
            print("\n" + "=" * 60)
            print("STEP 3: Re-create Kiro Subscriptions (target)")
            print("=" * 60)

            logging.info("Subscribing users...")

            def _subscribe(sub: dict) -> tuple[str, bool, str]:
                old_uid = sub["user_id"]
                new_uid = user_id_map.get(old_uid)
                username = user_id_to_name.get(old_uid, old_uid)
                if not new_uid:
                    return username, False, "user not mapped to target"
                api_type, _ = resolve_tier(sub["kiro_tier"])
                if not api_type:
                    return username, False, f"unknown tier: {sub['kiro_tier']}"
                ok, err = create_assignment(new_uid, api_type, target_credentials, region)
                return username, ok, err

            with ThreadPoolExecutor(max_workers=min(args.workers, len(subscriptions))) as pool:
                futures = {pool.submit(_subscribe, s): s for s in subscriptions}
                for fut in as_completed(futures):
                    username, ok, err = fut.result()
                    if ok:
                        logging.info("  Subscribed user: %s", username)
                        subs_created += 1
                    elif "ConflictException" in err:
                        logging.info("  Skipped (exists): %s", username)
                        subs_skipped += 1
                    else:
                        logging.error("  Failed: %s -- %s", username, err)
                        subs_failed_count += 1

        # 3b. Group subscriptions
        if not group_subscriptions:
            logging.info("No Kiro group subscriptions to migrate")
        else:
            logging.info("Subscribing groups...")
            for gs in group_subscriptions:
                old_gid = gs["group_id"]
                new_gid = group_id_map.get(old_gid)
                gname = group_id_to_name.get(old_gid, old_gid)
                if not new_gid:
                    logging.error("  Failed: group '%s' not mapped to target", gname)
                    group_subs_failed_count += 1
                    continue
                api_type, _ = resolve_tier(gs["kiro_tier"])
                if not api_type:
                    logging.error("  Failed: unknown tier '%s' for group '%s'", gs["kiro_tier"], gname)
                    group_subs_failed_count += 1
                    continue
                ok, err = create_assignment(
                    new_gid, api_type, target_credentials, region, principal_type="GROUP",
                )
                if ok:
                    logging.info("  Subscribed group: %s [%s]", gname, gs["kiro_tier"])
                    group_subs_created += 1
                elif "ConflictException" in err:
                    logging.info("  Skipped (exists): %s", gname)
                    group_subs_skipped += 1
                else:
                    logging.error("  Failed: %s -- %s", gname, err)
                    group_subs_failed_count += 1

    # ==================================================================
    # Final Summary
    # ==================================================================
    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    print(f"  Source : {args.source_profile} (Identity Store: {source_id_store})")
    print(f"  Target : {args.target_profile} (Identity Store: {target_id_store})")
    print()
    print(f"  Groups              : {groups_created} created, {groups_skipped} skipped")
    print(f"  Users               : {users_created} created, {users_skipped} skipped, {users_failed} failed")
    print(f"  Memberships         : {memberships_created} created, {memberships_skipped} skipped, {memberships_failed} failed")
    print(f"  Password Reset      : {reset_sent} sent, {reset_failed_count} failed")
    print(f"  User Subscriptions  : {subs_created} created, {subs_skipped} skipped, {subs_failed_count} failed")
    print(f"  Group Subscriptions : {group_subs_created} created, {group_subs_skipped} skipped, {group_subs_failed_count} failed")
    print("=" * 60)

    if users_failed or memberships_failed or reset_failed_count or subs_failed_count or group_subs_failed_count:
        sys.exit(1)

    logging.info("Migration complete.")


if __name__ == "__main__":
    main()
