# -*- coding: utf-8 -*-
"""텔레그램 채널 ID 찾기 — 초대 링크는 chat_id 가 아니다.

`https://t.me/+XXXX` 는 **초대 링크**이지 API 가 받는 `chat_id` 가 아니다.
채널의 chat_id 는 `-100` 으로 시작하는 숫자다.

쓰는 법
-------
  1. 새 채널에 봇을 **관리자로** 추가한다 (채널은 관리자여야 글을 쓸 수 있다)
  2. 그 채널에 아무 메시지나 하나 올린다
  3. 이 스크립트를 돌린다 → 보이는 채널의 이름과 chat_id 가 찍힌다
  4. 그 값을 GitHub secret `TELEGRAM_CHAT_ID` 에 넣는다

⚠️ 읽기만 한다. 메시지를 보내지 않는다.
⚠️ `getUpdates` 는 최근 것만 돌려주고, 웹훅이 걸려 있으면 비어 있을 수 있다.
   그 경우도 **이유를 말한다** — 빈 결과를 "채널 없음" 으로 읽지 않는다.
"""
import json
import os
import sys

import requests

API = "https://api.telegram.org/bot{token}/{method}"


def call(token, method, **params):
    """(결과, 사유). **사유를 반드시 남긴다.**"""
    try:
        r = requests.get(API.format(token=token, method=method),
                         params=params, timeout=15)
        j = r.json()
    except Exception as e:                     # noqa: BLE001
        return None, f"{type(e).__name__}: {str(e)[:120]}"
    if not j.get("ok"):
        return None, f"HTTP {r.status_code} · {j.get('description', '')}"
    return j.get("result"), ""


def chats_from_updates(updates):
    """업데이트에서 (chat_id, 종류, 제목) 을 중복 없이 뽑는다."""
    seen, out = set(), []
    for u in updates or []:
        for key in ("channel_post", "edited_channel_post", "message",
                    "edited_message", "my_chat_member"):
            chat = (u.get(key) or {}).get("chat")
            if not isinstance(chat, dict):
                continue
            cid = chat.get("id")
            if cid is None or cid in seen:
                continue
            seen.add(cid)
            out.append((cid, chat.get("type", "?"),
                        chat.get("title") or chat.get("username")
                        or chat.get("first_name") or ""))
    return out


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        print("❌ TELEGRAM_BOT_TOKEN 이 없다 (환경변수/secret 확인)")
        return 2

    me, why = call(token, "getMe")
    if me is None:
        print(f"❌ 봇 확인 실패: {why}")
        return 2
    print(f"🤖 봇: @{me.get('username')} ({me.get('first_name')})\n")

    hook, _ = call(token, "getWebhookInfo")
    if hook and hook.get("url"):
        print(f"⚠️ 웹훅이 걸려 있다({hook['url']}) — getUpdates 가 비어 있을 수 있다.\n")

    updates, why = call(token, "getUpdates", limit=100, timeout=0)
    if updates is None:
        print(f"❌ getUpdates 실패: {why}")
        return 2

    chats = chats_from_updates(updates)
    if not chats:
        print("📭 보이는 대화가 없다. **채널이 없다는 뜻이 아니다.** 가능한 이유:")
        print("   · 봇을 채널에 아직 추가하지 않았다(채널은 **관리자** 여야 한다)")
        print("   · 추가한 뒤 채널에 메시지를 올리지 않았다")
        print("   · getUpdates 는 최근 것만 준다 — 오래되면 사라진다")
        print("   · 웹훅이 걸려 있으면 업데이트가 그쪽으로 간다")
        return 1

    print("찾은 대화:")
    print(f"  {'chat_id':>16}  {'종류':<10} 이름")
    for cid, kind, title in chats:
        mark = "  ← 채널" if kind == "channel" else ""
        print(f"  {cid:>16}  {kind:<10} {title}{mark}")
    print("\n이 중 새 채널의 chat_id 를 GitHub secret `TELEGRAM_CHAT_ID` 에 넣는다.")
    print("(채널이면 보통 -100 으로 시작한다)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
