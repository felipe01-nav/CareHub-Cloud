#!/bin/sh
# CareHub: creates a test account (local testing only).
# Usage: sh create-user.sh alice@test.local SomePassword123
EMAIL="$1"
PASSWORD="$2"
if [ -z "$EMAIL" ] || [ -z "$PASSWORD" ]; then
  echo "Usage: sh create-user.sh email password"
  exit 1
fi
RESPONSE=$(curl -sS -X POST "http://localhost/gotrue/signup" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}")
case "$RESPONSE" in
  *access_token*|*\"id\"*) echo "OK: account created for $EMAIL" ;;
  *) echo "Something went wrong. Server said:"; echo "$RESPONSE"; exit 1 ;;
esac
