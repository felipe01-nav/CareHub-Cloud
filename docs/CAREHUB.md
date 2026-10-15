# CareHub notes

CareHub is a private, self-hosted workspace built from the open-source (AGPL-3.0)
AppFlowy-Cloud server (this repo) and AppFlowy-Web (CareHub-Web repo).

* Local setup guide: [`carehub/README.md`](../carehub/README.md)
* Server compatibility routes for the newer web client: `src/api/carehub_compat.rs`
* Image build: `.github/workflows/carehub-images.yml` → `ghcr.io/felipe01-nav/carehub-server` and `carehub-worker`
* The default `docker-compose.yml` in the repo root pulls commercial AppFlowy images – **do not use it**.
  Use `carehub/docker-compose.yml` instead.
