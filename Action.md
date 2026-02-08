# GitHub Actions

## Deploy Backend

**Workflow File**: `.github/workflows/deployServiceBackend.yml`

### Trigger
- Pushes to `main`.

### Description
Deploys the Backtest Hub backend service to the `neo-test1` server.

### Steps
1. **Deploy**: Uses `TBDTBD-git/terraform_config` reusable workflow.
2. **Commands**:
    - Navigates to `/home/trade/backtest-hub/`.
    - Stops existing containers (`docker-compose down`).
    - Rebuilds images (`docker-compose build --no-cache`).
    - Starts the service (`docker-compose up -d`).
    - Prunes unused Docker objects.

### Secrets Required
- `SSH_PRIVATE_KEY_NEO_TEST1_SERVER`
- `TS_AUTHKEY`
- `SLACK_BOT_TOKEN`
- `SLACK_CHANNEL_ID`
