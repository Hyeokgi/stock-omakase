#!/usr/bin/env bash
# 영수증 보존 — 워크플로 공통 계약 (2026-09-18 P0-1)
#
# 영수증은 각 runner 의 로컬 디스크에 떨어진다. 저장소에 넣지 않으면 runner 가
# 끝나는 순간 사라지고, Evidence Builder 가 요구하는 세 종류가 **동시에 존재할 수
# 없다.** 그러면 Gate 는 영원히 통과하지 못한다.
#
# 영수증은 건당 독립 파일이므로 워크플로끼리 같은 파일을 고치지 않는다.
# 그래도 커밋은 겹칠 수 있으므로 rebase 후 재시도한다.
set -u
KIND="${1:?사용법: receipt_commit.sh <kind>}"
if [ -z "$(git status --porcelain data/receipts 2>/dev/null)" ]; then
  echo "영수증 변경 없음 ($KIND)"
  exit 0
fi
git config user.name  "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git add -- data/receipts
git commit -m "receipt: $KIND $(TZ=Asia/Seoul date +%Y-%m-%d) [skip ci]" || exit 0
for i in 1 2 3 4; do
  git pull --rebase --autostash origin main && git push origin HEAD:main && exit 0
  echo "⚠️ 영수증 푸시 재시도 $i/4"
  sleep $((i * 3))
done
# 🔴 영수증을 못 남겨도 **생산을 실패로 만들지 않는다.** 관측 때문에 수집을 잃지 않는다.
#    대신 그 사이클은 증거가 없으므로 Gate 에서 자연히 떨어진다(조용한 통과가 없다).
echo "::warning::영수증 푸시 실패 ($KIND) — 이 사이클은 증거 부족으로 Gate 를 통과하지 못한다"
exit 0
