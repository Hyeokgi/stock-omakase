# -*- coding: utf-8 -*-
# ==========================================================================
# 🔎 한국투자증권 Open API '시황·공시 제목' 1회 시험 호출 (읽기 전용 · 저장 없음)
# --------------------------------------------------------------------------
# 사용자 지시 2026-10-08: "1,2번 실행해" — ② 한투 '시황·공시 제목' API 로 뉴스 층을 받을 수 있는지 확인.
# 하는 것: 접근토큰 1회 발급 → 시황·공시 제목 1회 조회 → **응답 구조만** 로그에 남긴다.
#   (응답 코드·건수·필드 이름·날짜/시각 범위·종목코드가 붙은 비율·제공처 코드 수)
# 하지 않는 것: 제목·종목명 출력, 저장·커밋, 반복 호출, 구글 시트 읽기·쓰기(운영 토큰 칸을 건드리지 않는다), 토큰 출력.
# 엔드포인트·tr_id 는 공식 문서로 확인하지 못한 기억값이다 — 응답이 오류면 그 자체가 확인 결과다.
# ==========================================================================
import datetime
import os
import sys

import requests

BASE = "https://openapi.koreainvestment.com:9443"
PATH = "/uapi/domestic-stock/v1/quotations/news-title"
TR_ID = "FHKST01011800"
PARAMS = {"FID_NEWS_OFER_ENTP_CODE": "", "FID_COND_MRKT_CLS_CODE": "", "FID_INPUT_ISCD": "", "FID_TITL_CNTT": "",
          "FID_INPUT_DATE_1": "", "FID_INPUT_HOUR_1": "", "FID_RANK_SORT_CLS_CODE": "", "FID_INPUT_SRNO": ""}
TITLE_KEYS = ("hts_pbnt_titl_cntt", "titl", "title")
NAME_PREFIX = ("kor_isnm",)


def shape(js):
    """응답에서 구조 요약만 뽑는다 — 제목·종목명 값은 버린다."""
    rows = js.get("output") or js.get("output1") or []
    if isinstance(rows, dict):
        rows = [rows]
    keys = sorted({k for r in rows for k in r})
    dts = sorted(f"{r.get('data_dt', '')} {r.get('data_tm', '')}".strip() for r in rows if r.get("data_dt"))
    with_code = sum(1 for r in rows if any((r.get(f"iscd{i}") or "").strip() for i in range(1, 11)))
    providers = {r.get("news_ofer_entp_code", "") for r in rows}
    return {"rt_cd": js.get("rt_cd"), "msg_cd": js.get("msg_cd"), "msg1": (js.get("msg1") or "")[:80],
            "rows": len(rows), "fields": keys, "first": dts[0] if dts else "", "last": dts[-1] if dts else "",
            "rows_with_stock_code": with_code, "provider_codes": len(providers - {""}),
            "has_title_field": any(k in keys for k in TITLE_KEYS),
            "has_name_fields": any(k.startswith(NAME_PREFIX) for k in keys),
            "has_continuation": bool(js.get("tr_cont") or js.get("ctx_area_fk100"))}


def main(env=None, post=requests.post, get=requests.get):
    env = os.environ if env is None else env
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    print(f"# 한투 시황·공시 제목 시험 호출 · {datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).isoformat(timespec='seconds')}")
    r = post(f"{BASE}/oauth2/tokenP", json={"grant_type": "client_credentials", "appkey": key, "appsecret": secret},
             headers={"content-type": "application/json"}, timeout=10)
    if r.status_code != 200 or not r.json().get("access_token"):
        print(f"❌ 토큰 발급 실패 HTTP {r.status_code} · {str(r.json().get('error_description', ''))[:80] if r.headers.get('content-type', '').startswith('application/json') else ''}")
        return 1
    token = r.json()["access_token"]
    h = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {token}", "appkey": key,
         "appsecret": secret, "tr_id": TR_ID, "custtype": "P"}
    q = get(BASE + PATH, headers=h, params=PARAMS, timeout=15)
    print(f"- HTTP {q.status_code} · tr_cont={q.headers.get('tr_cont', '')}")
    try:
        js = q.json()
    except ValueError:
        print("- JSON 아님 — 엔드포인트 확인 필요")
        return 1
    for k, v in shape(js).items():
        print(f"- {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
