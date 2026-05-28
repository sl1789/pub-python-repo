# DEPLOY — Phase 1 (containerize)

A single Docker image runs three services: **api** (FastAPI), **worker** (Databricks
poller), and **ui** (Streamlit). State lives in a named volume; secrets in `.env`.

## 1. Prerequisites

- Docker Desktop (macOS / Windows) or Docker Engine + Compose plugin (Linux/Pi).
- A copy of `.env.example` filled in as `.env` (see "Secrets" below).
- Optional but recommended: Azure Storage + Databricks credentials. Without them
  the app still runs — job submission and dataset routes degrade gracefully.

## 2. Configure secrets

```powershell
Copy-Item .env.example .env
# then edit .env
```

Required for a useful demo:

| Variable | Why |
|---|---|
| `JWT_SECRET` | Token signing — generate fresh: `python -c "import secrets;print(secrets.token_urlsafe(48))"` |
| `DEMO_USER_PASSWORD` | Login password for the seeded demo user |
| `AZURE_STORAGE_ACCOUNT` + `AZURE_STORAGE_KEY` | Read parquet datasets |
| `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_JOB_ID` | Submit MC jobs |

## 3. Build & run

```powershell
docker compose build
docker compose up -d
```

- **UI:** http://localhost:8501
- **API:** http://localhost:8000 (Swagger at `/docs`)
- Logs: `docker compose logs -f api` (or `worker`, `ui`)
- Stop: `docker compose down` &nbsp;·&nbsp; wipe state: `docker compose down -v`

The `api` runs `init_db_and_seed()` on startup, which creates the SQLite schema
in `/data/app.db` (mounted from the `appdata` named volume) and seeds the demo
user from `DEMO_USER_*`. The `worker` waits for the `api` healthcheck before
starting, so a clean boot is safe.

## 4. Architecture

```
┌────────┐  http  ┌────────┐  sqlite  ┌────────┐  http  ┌────────────┐
│  ui    ├───────►│  api   ├──────────┤  vol   ├────────┤  worker    │
│ :8501  │        │ :8000  │   /data  │ appdata│        │ (no port)  │
└────────┘        └────┬───┘          └────────┘        └─────┬──────┘
                       │                                       │
                       ▼                                       ▼
                 ┌──────────────┐                    ┌──────────────────┐
                 │  Azure ADLS  │                    │  Databricks Jobs │
                 │   (parquet)  │                    │   REST 2.0 API   │
                 └──────────────┘                    └──────────────────┘
```

All three containers share **one image** (`pub-python-repo:latest`) — they only
differ in their `command:`. This keeps build time and disk usage minimal.

## 5. Validation checklist

```powershell
# 1. Services are healthy
docker compose ps
# look for "healthy" on api and ui

# 2. API responds
curl http://localhost:8000/health

# 3. UI loads
Start-Process http://localhost:8501

# 4. Worker is polling (should log roughly every POLL_SECONDS)
docker compose logs --tail=20 worker
```

## 6. Notes & gotchas

- **Volume permissions.** The image runs as UID 1001. On Linux/Pi, the
  `appdata` named volume inherits that UID automatically. If you swap to a
  bind mount (`./data:/data`) make sure the host directory is writable by 1001.
- **Image size.** `pyspark` is a runtime dependency (~300 MB + JDK pulled by
  pip wheel). For the demo it's fine. For a slimmer Pi image, move `pyspark`
  to a separate `databricks` optional-dependency group and rebuild.
- **ARM64 / Raspberry Pi.** Build on the Pi with the same commands, or cross-
  build from your laptop: `docker buildx build --platform linux/arm64 -t pub-python-repo:arm64 .`
- **HTTPS.** Compose exposes plain HTTP. For anything beyond localhost use a
  reverse proxy with TLS (Caddy, Traefik, Cloudflare Tunnel) or deploy to a
  managed platform that terminates TLS for you (Phase 2).
- **State persistence.** `docker compose down` keeps `appdata`. Use
  `docker compose down -v` only when you want to wipe the SQLite + reseed.
- **Resetting the demo user password.** Edit `.env`, then `docker compose
  down -v && docker compose up -d` so the seed re-runs.

---

# Phase 2 — Deploy to Azure Container Apps (ACA)

## Mental model

ACA is a managed platform that runs your Docker images. You push the image to
Azure Container Registry (ACR) and create **one Container App per long-running
process**. ACA runs them, restarts them on crash, gives the public-facing one
an HTTPS URL with a managed TLS cert, and injects env vars / secrets from its
own secret store. **There is no agent inside the container talking back to
GitHub** — updates happen by pushing a new image tag and telling ACA to use it.

The three compose services map 1:1 to three Container Apps in the same
Environment:

| Container App | Ingress | Replicas | Purpose |
|---|---|---|---|
| `api` | internal | 1 | FastAPI, only reachable from inside the env |
| `worker` | none | 1 | Databricks poller, no inbound traffic |
| `ui` | external | 1 | Streamlit, gets the public HTTPS URL |

## 8. One-time Azure setup

```powershell
az extension add --name containerapp --upgrade
az login

az group create -n rg-mcdemo -l westeurope
az acr create  -n acrmcdemo -g rg-mcdemo --sku Basic --admin-enabled true
az containerapp env create -n cae-mcdemo -g rg-mcdemo -l westeurope
```

## 9. Build & push the image to ACR

ACR can build for you in the cloud — no local Docker needed:

```powershell
$tag = (git rev-parse --short HEAD)
az acr build --registry acrmcdemo --image pub-python-repo:$tag --file Dockerfile .
```

Alternative: `docker build` locally, then `az acr login; docker push`.

## 10. Create the three Container Apps

```powershell
$IMAGE = "acrmcdemo.azurecr.io/pub-python-repo:$tag"

# api — internal ingress
az containerapp create -n api -g rg-mcdemo --environment cae-mcdemo `
  --image $IMAGE --registry-server acrmcdemo.azurecr.io `
  --ingress internal --target-port 8000 `
  --min-replicas 1 --max-replicas 1 --cpu 0.5 --memory 1Gi `
  --command "uvicorn" --args "app.main:app" "--host" "0.0.0.0" "--port" "8000" "--proxy-headers"

# worker — no ingress
az containerapp create -n worker -g rg-mcdemo --environment cae-mcdemo `
  --image $IMAGE --registry-server acrmcdemo.azurecr.io `
  --min-replicas 1 --max-replicas 1 --cpu 0.25 --memory 0.5Gi `
  --command "python" --args "-m" "worker.worker"

# ui — external ingress (this is the public URL)
$API_INTERNAL = az containerapp show -n api -g rg-mcdemo `
  --query properties.configuration.ingress.fqdn -o tsv
az containerapp create -n ui -g rg-mcdemo --environment cae-mcdemo `
  --image $IMAGE --registry-server acrmcdemo.azurecr.io `
  --ingress external --target-port 8501 `
  --min-replicas 1 --max-replicas 1 --cpu 0.5 --memory 1Gi `
  --env-vars "API_BASE=https://$API_INTERNAL" `
  --command "streamlit" `
  --args "run" "ui/Monte_Carlo_Option_Pricing.py" "--server.address=0.0.0.0" "--server.port=8501" "--server.headless=true"
```

Differences vs `docker compose` worth knowing:

- **No shared filesystem by default.** For a demo, accept ephemeral SQLite —
  the API re-creates schema + reseeds demo user every time `api` restarts. For
  persistence, attach an Azure Files share via
  `az containerapp env storage set` and mount it on both `api` and `worker`,
  or switch `DATABASE_URL` to Azure Database for PostgreSQL Flexible Server.
- **min-replicas = 1** keeps the worker always running. Setting it to 0 saves
  money but stops job polling.
- **Internal DNS.** Inside the env, apps reach each other at
  `https://<appname>.internal.<env-default-domain>` with managed TLS.

## 11. Access the UI

```powershell
az containerapp show -n ui -g rg-mcdemo `
  --query properties.configuration.ingress.fqdn -o tsv
# -> ui.<random>.<region>.azurecontainerapps.io
```

Open `https://<that-fqdn>/` in any browser. Log in with the `DEMO_USER_*`
credentials. Custom domain + free managed cert can be attached later via the
portal.

## 12. Secrets — how it works end-to-end

Each Container App has its own encrypted secret store. The pattern is:

1. **Set the secret** on the app (sent over TLS, never echoed back).
2. **Bind it** to an env var using `secretref:` — your code keeps using
   `os.getenv(...)` unchanged.

```powershell
# 1. store secrets on the api app
az containerapp secret set -n api -g rg-mcdemo --secrets `
  jwt-secret="$(python -c 'import secrets;print(secrets.token_urlsafe(48))')" `
  demo-password="ChangeMe!" `
  azure-storage-key="<adls key>" `
  databricks-token="<dbx token>"

# 2. bind them as env vars (non-secret config is plain)
az containerapp update -n api -g rg-mcdemo --set-env-vars `
  APP_ENV=prod `
  JWT_SECRET=secretref:jwt-secret `
  DEMO_USER_USERNAME=demo `
  DEMO_USER_PASSWORD=secretref:demo-password `
  AZURE_STORAGE_ACCOUNT=<account-name> `
  AZURE_STORAGE_KEY=secretref:azure-storage-key `
  AZURE_RESULTS_CONTAINER=results `
  AZURE_RESULTS_PREFIX=export `
  DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net `
  DATABRICKS_TOKEN=secretref:databricks-token `
  DATABRICKS_JOB_ID=<job id>
```

Rules:

- Secrets are **scoped to one Container App**. Re-set them on `worker` and
  `ui` if those apps need them too (the worker needs JWT + storage + dbx; the
  ui needs nothing secret if API_BASE is internal).
- Secret values **never** appear in `az containerapp show` output.
- Long-term pattern: store secrets in Azure Key Vault and reference them via
  Managed Identity. Overkill for a few-day demo.
- Your local `.env` is only for `docker compose`. Azure never sees it. Keep it
  in `.gitignore` (already done).

## 13. The deploy-on-change cycle

Push to git → rebuild image → tell ACA to use the new tag. The container
has no live link to GitHub.

```powershell
$tag = (git rev-parse --short HEAD)
az acr build --registry acrmcdemo --image pub-python-repo:$tag .

"api","worker","ui" | ForEach-Object -Parallel {
  az containerapp update -n $_ -g rg-mcdemo `
    --image "acrmcdemo.azurecr.io/pub-python-repo:$using:tag"
}
```

Tag with `:<git-sha>` (not `:latest`) so rollback is one command:
`az containerapp update --image ...:<previous-sha>`.

A GitHub Actions workflow can run the same two commands on every push to
`main` — the runner does the build, the container still doesn't touch git.

## 14. ACA cost, logs, lifecycle

- **Cost.** Billed per vCPU-second + GiB-second of active time. Three small
  apps at min-replicas=1 ≈ $20–35 / month if left on 24/7; a 5-day demo is
  well under $10. ACA also has a generous monthly free grant.
- **Cold start.** `--min-replicas 0` saves money between demos but the first
  request after idle takes 5–20 s. Keep the worker at 1 if you want jobs
  polled while you sleep.
- **Logs.** `az containerapp logs show -n api -g rg-mcdemo --follow` is your
  `docker compose logs -f`. Also streamed to Log Analytics automatically.
- **Tear-down.** `az group delete -n rg-mcdemo --yes --no-wait` nukes
  everything and stops billing immediately.

## 15. Operational loop summary

```text
First time:
  az login → create rg + acr + env → az acr build
  → 3× az containerapp create → secret set + env vars
  → open the ui FQDN

Every code change:
  git push
  az acr build --image pub-python-repo:$(git rev-parse --short HEAD) .
  az containerapp update --image ...:<tag>     # × 3 apps

End of demo:
  az group delete -n rg-mcdemo --yes --no-wait
```

