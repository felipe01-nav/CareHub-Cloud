#!/bin/sh
# CareHub: creates the .env file with random passwords (run once).
set -e
cd "$(dirname "$0")"
if [ -f .env ]; then
  echo "OK: .env already exists - keeping your existing passwords."
  exit 0
fi
cat > .env <<ENV
# CareHub local secrets - created by setup.sh. Do not share this file.
POSTGRES_PASSWORD=$(openssl rand -hex 24)
GOTRUE_JWT_SECRET=$(openssl rand -hex 32)
MINIO_USER=carehub
MINIO_PASSWORD=$(openssl rand -hex 24)
ENV
echo "OK: created .env with random passwords."
