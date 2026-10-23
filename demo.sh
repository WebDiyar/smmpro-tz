#!/usr/bin/env bash
# Сценарий руками: оплата (дважды), заказ, статус. Нужен запущенный `docker compose up`.
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] && set -a && . ./.env && set +a
API=${API:-http://localhost:${API_PORT:-8000}}
SECRET=${WEBHOOK_SECRET:-test-secret}
sign() { printf %s "$1" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}'; }
post() { curl -s "$API$1" -H "X-Signature: $(sign "$2")" -H 'Content-Type: application/json' -d "$2"; echo; }

PAY="demo-$(date +%s)"
BODY="{\"payment_id\":\"$PAY\",\"user_id\":42,\"tokens\":10,\"status\":\"succeeded\"}"
echo "1. оплата 10 токенов (ждём credited:true):";  post /webhooks/payments "$BODY"
echo "2. та же оплата ещё раз (credited:false):";   post /webhooks/payments "$BODY"

echo "3. заказ image-fast (цена 2):"
TASK=$(curl -s "$API/generate" -H 'Content-Type: application/json' \
  -d '{"user_id":42,"model_slug":"image-fast","prompt":"cat","ref_images":[]}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["task_id"])')
echo "task_id=$TASK"

echo "4. статус (фейк отвечает через 2-20 с), опрашиваем:"
for _ in $(seq 30); do
  S=$(curl -s "$API/tasks/$TASK"); echo "$S" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])'
  case "$S" in *'"status":"completed"'*|*'"status":"failed"'*) break;; esac; sleep 2
done
echo "$S" | python3 -m json.tool
echo "5. баланс:"; curl -s "$API/users/42/balance" | python3 -m json.tool
