# Kiro Subscription & Identity Center Manager

Manage AWS Identity Center users and Kiro subscriptions at scale -- bulk import, password reset, and cross-account migration, all via direct API calls.

## Use Cases

### Use Case 1: Bulk Import New Users

You have a list of users (e.g. a team roster) and want to:
1. Create them in AWS Identity Center
2. Send them password reset emails so they can log in
3. Subscribe them to Kiro tiers

**See: [Bulk Import Workflow](#use-case-1-bulk-import-workflow)**

### Use Case 2: Migrate Kiro Subscriptions Across AWS Accounts

You have Kiro set up in one AWS account and want to replicate the entire setup (Identity Store users, groups, memberships, and Kiro subscriptions) to another account.

**See: [Migration Workflow](#use-case-2-migration-workflow)**

## Prerequisites

- Python 3.10+
- AWS CLI configured with appropriate credentials
- For migration: AWS credentials for both source and target accounts (use `--profile`)
- IAM permissions: `identitystore:*`, `sso:ListInstances`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Tools

| Script | Description |
|--------|-------------|
| `idc_manager.py create-users` | Bulk create users from a CSV file |
| `idc_manager.py reset-password` | Send password reset emails |
| `idc_manager.py enrich` | Enrich a Kiro subscription export with user details |
| `idc_manager.py export-subscriptions` | Export Kiro subscriptions with enriched user details |
| `idc_manager.py export-store` | Export all users, groups, memberships from an Identity Store |
| `idc_manager.py import-store` | Import users, groups, memberships into another Identity Store |
| `kiro_subscribe.py` | Subscribe users to Kiro tiers (Pro, Pro+, Power) |
| `kiro_migrate.py` | **One-command migration** -- automates all 5 steps |

---

## Use Case 1: Bulk Import Workflow

### Step 1: Prepare a CSV

Create a CSV file with your users. See `sample_users.csv`:

```csv
UserName,GivenName,FamilyName,DisplayName,Email,Groups,KiroTier
jsmith,John,Smith,John Smith,jsmith@example.com,KiroUsers,Kiro Pro
jdoe,Jane,Doe,Jane Doe,jdoe@example.com,"KiroUsers,KiroAdmins",Kiro Pro+
bwilson,Bob,Wilson,Bob Wilson,bwilson@example.com,KiroUsers,Kiro Power
```

Column names are flexible (case-insensitive). Supported aliases:

| Field | Accepted Column Names |
|-------|----------------------|
| UserName | `UserName`, `username`, `user_name` (falls back to Email) |
| GivenName | `GivenName`, `given_name`, `first_name`, `firstname` |
| FamilyName | `FamilyName`, `family_name`, `last_name`, `lastname` |
| DisplayName | `DisplayName`, `display_name` (auto-generated if omitted) |
| Email | `Email`, `email`, `email_address` **(required)** |
| Groups | `Groups`, `groups`, `group_names` (comma-separated, optional) |
| KiroTier | `KiroTier`, `kiro_tier`, `tier`, `plan` (optional) |

### Step 2: Create Users + Send Password Reset

```bash
# Dry run first
python idc_manager.py create-users users.csv --dry-run

# Create users and send password reset emails in one step
python idc_manager.py create-users users.csv \
  --region us-east-1 \
  --output report.json \
  --reset-password
```

Users who already exist are skipped. The `--reset-password` flag sends a password setup email to each newly created user immediately after creation. **The password reset link expires in 1 hour.** If a user misses the window, re-run `idc_manager.py reset-password` to send a new link.

### Step 3: Subscribe to Kiro

```bash
# Per-user tier from CSV (KiroTier column)
python kiro_subscribe.py --csv users.csv --region us-east-1

# Or same tier for everyone
python kiro_subscribe.py --csv users.csv --tier "Kiro Pro+" --region us-east-1

# Or from the report (all same tier)
python kiro_subscribe.py --report report.json --tier "Kiro Pro" --region us-east-1
```

### Full Example

```bash
source .venv/bin/activate

# 1. Create users + password reset
python idc_manager.py create-users users.csv \
  --region us-east-1 \
  --output report.json \
  --reset-password

# 2. Subscribe to Kiro
python kiro_subscribe.py --csv users.csv --region us-east-1
```

---

## Use Case 2: Migration Workflow

Migrate Kiro subscriptions from Account A to Account B. This copies the Identity Store (users, groups, memberships) and re-creates the Kiro subscriptions.

**Before you start**: Make sure Kiro is enabled in the target account. Go to the target account's AWS Console > **Kiro** and complete the initial setup. The `CreateAssignment` API will fail with `AccessDeniedException` if Kiro is not activated.

### One-Command Migration (recommended)

```bash
# Dry run -- preview what will be migrated
python kiro_migrate.py \
  --source-profile source-account \
  --target-profile target-account \
  --region us-east-1 \
  --dry-run

# Run the migration
python kiro_migrate.py \
  --source-profile source-account \
  --target-profile target-account \
  --region us-east-1
```

This single command automates all 5 steps:

| Step | Account | What |
|------|---------|------|
| 1a | Source | Export Identity Store (users, groups, memberships) |
| 1b | Source | Export Kiro subscriptions |
| 2a | Target | Import Identity Store |
| 2b | Target | Send password reset emails |
| 3 | Target | Re-create Kiro subscriptions |

All steps are idempotent -- safe to re-run if interrupted.

#### CLI Options

| Option | Description |
|--------|-------------|
| `--source-profile` | AWS CLI profile for the source account (required) |
| `--target-profile` | AWS CLI profile for the target account (required) |
| `--region`, `-r` | AWS region (default: `us-east-1`) |
| `--dry-run`, `-n` | Preview all steps without making changes |
| `--skip-reset-password` | Skip sending password reset emails |
| `--skip-subscriptions` | Skip re-creating Kiro subscriptions |
| `--workers`, `-w` | Parallel workers (default: `5`) |
| `--verbose`, `-v` | Enable debug logging |

### Step-by-Step Migration (manual control)

### Step 1: Export from the Source Account

**1a. Export the Identity Store** (users, groups, memberships):

```bash
python idc_manager.py export-store \
  --profile source-account \
  --region us-east-1 \
  -o identity-store-backup.json
```

**1b. Export Kiro Subscriptions** (with enriched user details):

```bash
python idc_manager.py export-subscriptions \
  --profile source-account \
  --region us-east-1 \
  -o users.csv
```

This calls the `ListUserSubscriptions` API to fetch all Kiro subscriptions, then enriches each entry with user details (name, email, groups) from the source Identity Store. The output `users.csv` is used in subsequent steps.

### Step 2: Import into the Target Account

**2a. Import the Identity Store** (groups, users, memberships):

```bash
# Preview first
python idc_manager.py import-store identity-store-backup.json \
  --profile target-account \
  --region us-east-1 \
  --dry-run

# Import for real
python idc_manager.py import-store identity-store-backup.json \
  --profile target-account \
  --region us-east-1
```

This creates all groups, users, and memberships in the target Identity Store. Existing users/groups are skipped (idempotent). Import order: groups -> users -> memberships.

**2b. Send Password Reset Emails**:

Users created via API don't receive a password email. Send one to each user in the target account. **The password reset link expires in 1 hour.** If a user misses the window, re-run this command to send a new link:

```bash
python idc_manager.py reset-password \
  --csv users.csv \
  --profile target-account \
  --region us-east-1
```

### Step 3: Re-create Kiro Subscriptions

Subscribe users to their Kiro tiers in the target account:

```bash
python kiro_subscribe.py \
  --csv users.csv \
  --profile target-account \
  --region us-east-1
```

### Full Migration Example

```bash
source .venv/bin/activate

# === Source Account ===

# 1a. Export Identity Store
python idc_manager.py export-store \
  --profile source-account \
  --region us-east-1 \
  -o identity-store-backup.json

# 1b. Export Kiro subscriptions (with user details)
python idc_manager.py export-subscriptions \
  --profile source-account \
  --region us-east-1 \
  -o users.csv

# === Target Account ===

# 2a. Import Identity Store (groups, users, memberships)
python idc_manager.py import-store identity-store-backup.json \
  --profile target-account \
  --region us-east-1

# 2b. Send password reset emails
python idc_manager.py reset-password \
  --csv users.csv \
  --profile target-account \
  --region us-east-1

# 3. Re-create Kiro subscriptions
python kiro_subscribe.py \
  --csv users.csv \
  --profile target-account \
  --region us-east-1
```

---

## Kiro Tiers

| Tier Name | CLI Value | CSV Value | Price | Credits |
|-----------|-----------|-----------|-------|---------|
| Kiro Pro | `pro` | `Kiro Pro` | $20/mo | 1,000 |
| Kiro Pro+ | `pro+` | `Kiro Pro+` | $40/mo | 2,000 |
| Kiro Power | `power` | `Kiro Power` | $200/mo | 10,000 |

Short aliases (`pro`, `pro+`, `power`) are accepted as CLI input and in CSV files. All outputs use the canonical names (`Kiro Pro`, `Kiro Pro+`, `Kiro Power`).

## Command Reference

Run `--help` on any command for full details:

```bash
python idc_manager.py --help
python idc_manager.py create-users --help
python idc_manager.py reset-password --help
python idc_manager.py enrich --help
python idc_manager.py export-subscriptions --help
python idc_manager.py export-store --help
python idc_manager.py import-store --help
python kiro_subscribe.py --help
python kiro_migrate.py --help
```

## Notes

- **Idempotent**: All operations skip existing resources (users, groups, memberships, subscriptions).
- **Parallel**: `reset-password` and `kiro_subscribe.py` support `--workers` for parallel execution (default: 5).
- **No browser required**: All operations use direct AWS API calls with SigV4 signing.
- **Internal APIs**: `reset-password` uses `SWBUPService.UpdatePassword` (service: `userpool`). `kiro_subscribe.py` uses `AmazonQDeveloperService.CreateAssignment` (service: `q`). `export-subscriptions` uses `AWSZornControlPlaneService.ListUserSubscriptions` (service: `user-subscriptions`). These are internal AWS APIs discovered via network traffic analysis.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `No AWS Identity Center instance found` | Enable Identity Center in the account, or specify `--region` |
| `ConflictException` on user creation | User already exists -- check the "skipped" count |
| `AccessDeniedException` on Kiro subscription | Kiro is not enabled in the target account. Go to AWS Console > Kiro and complete the initial setup first |
| `ConflictException` on Kiro subscription | User already subscribed to a tier |
| `HTTP 403` | Check AWS credentials and IAM permissions |
| User not found when using `--csv` | Username in CSV must match the username in Identity Center |
| Invalid tier name | Use `pro`, `pro+`, `power` or `Kiro Pro`, `Kiro Pro+`, `Kiro Power` |
| Same Identity Store error on import | Use `--force` to import into the same store, or specify a different `--identity-store-id` |

## License

MIT
