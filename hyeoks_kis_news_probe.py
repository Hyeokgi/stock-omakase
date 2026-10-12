# -*- coding: utf-8 -*-
# ==========================================================================
# 🔎 한국투자증권 Open API '시황·공시 제목' 1회 시험 호출 (읽기 전용 · 저장 없음)
# --------------------------------------------------------------------------
# 사용자 지시 2026-10-08: "1,2번 실행해" — ② 한투 '시황·공시 제목' API 로 뉴스 층을 받을 수 있는지 확인.
# 하는 것: 접근토큰 1회 발급 → 시황·공시 제목 1회 조회 → **응답 구조만** 로그에 남긴다.
#   (응답 코드·건수·필드 이름·날짜/시각 범위·종목코드가 붙은 비율·제공처 코드 수)
# 하지 않는 것: 제목·종목명 출력, 저장·커밋, 반복 호출, 구글 시트 읽기·쓰기(운영 토큰 칸을 건드리지 않는다), 토큰 출력.
#   예외: `--continuation`(2026-10-12, 허브 #19)은 이어 받기 입력 후보 형식마다 1회씩 부른다(최대 약 11회).
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


# 🔎 2026-10-12 (허브 #19, 사용자 결정 "넘침 때만 이어 받기 — 장 마감 뒤 1회 시험 먼저")
#    5분에 40건이 넘치면 그 사이 뉴스를 놓친다. 같은 API 의 시각·일련번호 입력으로 더 이전 목록을
#    받을 수 있는지 확인한다. 입력 형식은 공식 문서로 확인하지 못해 후보 형식을 모두 한 번씩 부른다.
#    출력은 구조만 — 건수·시각 범위·첫 목록과 겹친 수·첫 목록보다 이전인 수. 제목·종목명은 찍지 않는다.
def _pub(r):
    d, t = (r.get("data_dt") or "").strip(), (r.get("data_tm") or "").strip().rjust(6, "0")
    return f"{d} {t}" if len(d) == 8 else ""


def _key(r):
    return (r.get("cntt_usiq_srno") or "").strip() or _pub(r) + "|" + (r.get("news_ofer_entp_code") or "")


def _rows(js):
    rows = js.get("output") or js.get("output1") or []
    return [rows] if isinstance(rows, dict) else rows


def continuation_variants(oldest):
    """첫 목록의 가장 오래된 행 → (이름, 바뀐 입력, 추가 헤더) 후보들."""
    dt = (oldest.get("data_dt") or "").strip()
    tm = (oldest.get("data_tm") or "").strip().rjust(6, "0")
    sn = (oldest.get("cntt_usiq_srno") or "").strip()
    return [
        ("헤더 tr_cont=N", {}, {"tr_cont": "N"}),
        ("날짜 00YYYYMMDD·시각 0000HHMMSS", {"FID_INPUT_DATE_1": "00" + dt, "FID_INPUT_HOUR_1": "0000" + tm}, {}),
        ("날짜 YYYYMMDD·시각 HHMMSS", {"FID_INPUT_DATE_1": dt, "FID_INPUT_HOUR_1": tm}, {}),
        ("날짜·시각(00 형식)+일련번호", {"FID_INPUT_DATE_1": "00" + dt, "FID_INPUT_HOUR_1": "0000" + tm,
                                    "FID_INPUT_SRNO": sn}, {}),
        ("시각만 0000HHMMSS", {"FID_INPUT_HOUR_1": "0000" + tm}, {}),
    ]


def compare(base_rows, rows):
    """두 목록의 구조 비교 — 값은 시각과 건수뿐이다."""
    base_keys = {_key(r) for r in base_rows}
    base_old = min((_pub(r) for r in base_rows if _pub(r)), default="")
    pubs = sorted(p for p in (_pub(r) for r in rows) if p)
    return {"rows": len(rows), "first": pubs[0] if pubs else "", "last": pubs[-1] if pubs else "",
            "overlap_with_base": sum(1 for r in rows if _key(r) in base_keys),
            "older_than_base": sum(1 for p in pubs if base_old and p < base_old),
            "not_older": sum(1 for p in pubs if base_old and p >= base_old)}


def continuation(get, h, base_rows, pause=0.3, sleep=None):
    """후보 형식마다 1회 호출. 이어 받기로 보이면 그 형식으로 한 번 더 거슬러 간다."""
    import time
    sleep = sleep or time.sleep
    out, base_old = [], min((r for r in base_rows if _pub(r)), key=_pub, default=None)
    if base_old is None:
        print("- 첫 목록에 시각이 없다 — 이어 받기 시험 불가")
        return out
    print(f"- 첫 목록: {len(base_rows)}건 · 가장 오래된 행 {_pub(base_old)}")
    for name, extra, hdr in continuation_variants(base_old):
        sleep(pause)
        q = get(BASE + PATH, headers=dict(h, **hdr), params=dict(PARAMS, **extra), timeout=15)
        try:
            js = q.json()
        except ValueError:
            print(f"- [{name}] HTTP {q.status_code} · JSON 아님")
            continue
        rows = _rows(js)
        c = compare(base_rows, rows)
        works = c["rows"] > 0 and c["older_than_base"] >= c["rows"] - 1 and c["overlap_with_base"] <= 1
        out.append((name, works))
        print(f"- [{name}] HTTP {q.status_code} · rt_cd={js.get('rt_cd')} {js.get('msg_cd', '')} · "
              f"tr_cont={q.headers.get('tr_cont', '')} · {c} · 이어 받기로 보임={works}")
        if works:
            older = min((r for r in rows if _pub(r)), key=_pub)
            name2, extra2, hdr2 = next(v for v in continuation_variants(older) if v[0] == name)
            sleep(pause)
            q2 = get(BASE + PATH, headers=dict(h, **hdr2), params=dict(PARAMS, **extra2), timeout=15)
            c2 = compare(rows, _rows(q2.json()))
            print(f"  ↳ 한 번 더 거슬러: {c2}")
    return out


def main(env=None, post=requests.post, get=requests.get, argv=None):
    mode = "continuation" if "--continuation" in (sys.argv[1:] if argv is None else argv) else "basic"
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
    if mode == "continuation" and str(js.get("rt_cd")) == "0":
        print("\n## 이어 받기 시험 (허브 #19)")
        continuation(get, h, _rows(js))
    return 0


if __name__ == "__main__":
    sys.exit(main())
