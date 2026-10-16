# CareHub – run it on your Mac (local testing)

> **Fictional data only.** This setup is for personal testing on your own Mac at
> `http://localhost`. Nothing is published to the internet.

## What's in this folder

| File | What it does |
|---|---|
| `docker-compose.yml` | Starts all CareHub parts in Docker |
| `nginx.conf` | Sends browser traffic to the right part |
| `postgres-init.sql` | Prepares the database the first time |
| `setup.sh` | Creates `.env` with random passwords (run once) |
| `create-user.sh` | Creates a test account |

## One-time setup

1. **Docker Desktop** is installed and running (whale icon in the menu bar).
   Settings → General: make sure **"Use Rosetta for x86_64/amd64 emulation on Apple Silicon"** is ticked.
2. **Create a read-only GitHub token** so Docker can download your private CareHub images:
   open <https://github.com/settings/tokens/new?scopes=read:packages&description=CareHub%20Docker>,
   set an expiry, keep only **read:packages** ticked → **Generate token** → copy it.
3. **Get this folder onto your Mac:** on the CareHub-Cloud repo page click **Code → Download ZIP**,
   then in Terminal:
   ```sh
   cd ~/Downloads && unzip -q CareHub-Cloud-main.zip
   mv ~/Downloads/CareHub-Cloud-main/carehub ~/carehub
   cd ~/carehub
   ```
4. **Log Docker in to GitHub** (paste the token when asked for a password – nothing shows while you paste, that's normal):
   ```sh
   docker login ghcr.io -u felipe01-nav
   ```
5. **Create your passwords file:**
   ```sh
   sh setup.sh
   ```

## Start CareHub

```sh
cd ~/carehub
docker compose up -d
docker compose ps
```

The first start downloads everything (a few minutes). Run `docker compose ps` again until
`appflowy_cloud` shows **healthy**.

## Create two test users

```sh
sh create-user.sh alice@test.local CareHub-Test-1
sh create-user.sh bob@test.local CareHub-Test-2
```

Open <http://localhost> and sign in with **email + password**.

## Everyday commands

| Task | Command |
|---|---|
| Start | `docker compose up -d` |
| Stop (keeps data) | `docker compose down` |
| Status | `docker compose ps` |
| Server log (for error reports) | `docker compose logs --tail 100 appflowy_cloud` |
| Sign-in log | `docker compose logs --tail 100 gotrue` |
| Get the newest CareHub build | `docker compose pull && docker compose up -d` |
| **Delete all data** (start fresh) | `docker compose down -v` |

## Known limits of this test setup

- No e-mail: invite people with an **invite link** (Settings → Members), not by e-mail.
- AI, search, forms, charts and version history are switched off for now.
- Your data lives in Docker volumes on your Mac; `docker compose down -v` deletes it
  (the daily backups in `~/carehub/backups` are kept).

## Backups (automatic)

The `backup` service saves the database **and** uploaded files once a day into
`~/carehub/backups` and keeps the last 14 days. Check it is working:

```sh
cd ~/carehub && ls -lh backups && docker compose logs --tail 5 backup
```

Copy that folder to an external drive or iCloud now and then – a backup on the
same Mac doesn't help if the Mac dies.

**Restore a backup** (replaces the current data – ask first if unsure):

```sh
cd ~/carehub
docker compose stop appflowy_cloud appflowy_worker
docker compose exec -T postgres pg_restore -U postgres -d postgres --clean --if-exists < backups/carehub-db-YYYY-MM-DD_HHMM.dump
docker compose start appflowy_cloud appflowy_worker
```

## Opening CareHub from your phone (Tailscale)

1. Install Tailscale on the Mac and the phone, signed in to the same account.
2. In `~/carehub/.env` add (with your Mac's Tailscale name):
   ```
   CAREHUB_URL=https://your-mac.your-tailnet.ts.net
   CAREHUB_WS_URL=wss://your-mac.your-tailnet.ts.net
   ```
3. `tailscale serve --bg 80` then `docker compose up -d`.
4. Open the `https://…ts.net` address on the Mac and on the phone.
