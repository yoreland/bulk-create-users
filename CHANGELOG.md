# Changelog

## [Enhanced] - 2026-04-14

### Added
- **`kiro_unsubscribe.py`** - New tool to remove Kiro subscriptions from users
  - Supports single user (`--username`) or batch (`--csv`)
  - Dry-run mode (`--dry-run`) for safe preview
  - Automatic filtering of users without subscriptions
  - Note: Deletion enters "pending removal" state, effective at end of billing month

### Changed
- **`kiro_subscribe.py`** - Upgraded to support subscription lifecycle management
  - Switched from `CreateAssignment` to `UpdateAssignment` API
  - Now supports create, upgrade, and downgrade in a single operation
  - No more `ConflictException` when modifying existing subscriptions
  - Correctly uses SigV4 service name `q` (not `codewhisperer`)

### Fixed
- Fixed SigV4 signing: Service name must be `q` for write operations, not `codewhisperer`
- Improved error handling for subscription operations
- Better user feedback when subscriptions already exist

### Technical Details

**API Changes:**
```diff
- X-Amz-Target: AmazonQDeveloperService.CreateAssignment
+ X-Amz-Target: AmazonQDeveloperService.UpdateAssignment
```

**Behavior:**
- `UpdateAssignment` handles both new subscriptions and modifications
- For new users: Creates subscription
- For existing users: Upgrades or downgrades to new tier
- Single API call, no need to delete and recreate

**New API:**
```
X-Amz-Target: AmazonQDeveloperService.DeleteAssignment
Endpoint: https://codewhisperer.{region}.amazonaws.com/
SigV4 Service: q
```

### Testing
All changes have been validated in production environment:
- ✅ Add subscription (new users)
- ✅ Upgrade subscription (Pro → Pro+)
- ✅ Downgrade subscription (Pro+ → Pro)
- ✅ Remove subscription (deletion with pending removal state)
- ✅ Query subscription status

### Documentation
- Updated README.md with new "Subscription Management" section
- Added usage examples for upgrade/downgrade/remove operations
- Clarified API changes and behavior
- Removed obsolete `ConflictException` troubleshooting entry

---

## Original Release

See original repository: https://github.com/kiro-community/bulk-create-users
