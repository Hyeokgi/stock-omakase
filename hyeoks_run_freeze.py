"""실행 입력 동결 — 그날 무엇을 보고 골랐는지를 남긴다.

왜 만들었나 (2026-09-08, 외부 4차 검토)
---------------------------------------
`hyeoks_analyst.py` 는 매일 후보 풀 150종목을 만들어 AI 에게 넘기고 **버렸다.**
그래서 "같은 후보군에서 규칙이 골랐다면 무엇을 골랐을까"를 사후에 재현할 수 없었다.
로드맵 §4-5 의 A2(규칙 선정 vs AI 선정)는 **원리적으로 불가능**했고, A1 도 그날의
점수 상태를 복원할 수 없어 반쪽이었다.

같은 이유로 Phase 2 의 원시 일봉도 없어 청산 규칙 수정의 실표본 영향을 못 쟀다.
외부 검토가 짚은 그대로다 —
"'고정 입력으로 재계산'을 지키려면 **먼저 그 실행 입력을 저장하고 재사용**해야 한다."

보존 위치 — 구글 드라이브(비공개)
---------------------------------
⚠️ 이 저장소는 **공개**다. 후보 풀에는 V1·V2·V3·RS 점수와 타점 판정이 종목마다
붙어 있어 **시스템의 신호 그 자체**다. 공개 저장소에 커밋하면 git 히스토리에
영구히 남아 되돌릴 수 없다(2026-09-08 마스터 결정).
→ 이미 검증된 PDF 업로드 경로(GAS 웹앱)를 재사용해 **드라이브 비공개**로 올린다.

⚠️ 보존 실패를 성공처럼 넘기지 않는다. `freeze_status.json` 에 결과를 남기고,
   워크플로가 그 파일을 읽어 **실패면 잡을 빨갛게** 만든다. 리포트 발송은 이미
   끝난 뒤이므로 사용자는 PDF 를 받고, 대신 "사람이 봐야 한다"가 남는다.
"""
import base64
import datetime
import gzip
import hashlib
import json
import os
import time

KST = datetime.timezone(datetime.timedelta(hours=9))
FREEZE_VERSION = "run-freeze-v1"
STATUS_FILE = "freeze_status.json"
LOCAL_DIR = "data/run_freeze"       # freeze(local_dir=...) 가 러너에 남기는 사본. .gitignore 대상이다

# 🔴 2026-10-03 — 일시 오류 재시도.
#    10/2 15:02 동결 업로드가 비JSON 응답 **한 번**으로 실패했고, 그 하나로 워크플로가 빨개져
#    그날 Gate 가 FAIL 로 박제됐다. 같은 창구가 3분 뒤(15:05:33)에는 배지 보관을 4초 만에
#    받았다 — 일시 오류였다. 재시도는 두 단계로 나눈다.
#      · 호출 중(inline): 짧게. 리포트 생성보다 앞서 호출되므로 길게 기다리면 리포트가 늦어진다.
#      · 리포트를 다 보낸 뒤(late): 길게. 워크플로의 별도 단계가 러너에 남은 사본을 다시 올린다.
#    한 번 더 올린 파일은 이름이 내용 해시를 포함해 이름·바이트가 같다. 응답만 잃고 서버에는
#    저장된 경우 드라이브에 같은 이름의 같은 파일이 둘 생길 수 있다 — 내용은 동일하다.
UPLOAD_BACKOFF_INLINE = (5, 15)       # 최대 3회 시도
UPLOAD_BACKOFF_LATE = (20, 45, 90)    # 최대 4회 시도
LATE_BUDGET_SECONDS = 300             # 후속 재시도 전체 상한 — 잡 timeout-minutes 안에서 끝낸다

# 드라이브 업로드 창구. PDF 업로드가 쓰던 것과 **같은 웹앱**이라 경로가 이미 검증돼 있다.
# ⚠️ 이 URL 은 원래 hyeoks_analyst.py 에 하드코딩돼 있었고 저장소가 공개라 이미
#    노출된 값이다. 여기로 옮긴 것은 두 스크립트가 같은 값을 쓰게 하려는 것이지
#    노출 상태를 바꾸지 않는다 — 이 창구는 **쓰기 전용**이고 읽기·목록은 안 된다.
DEFAULT_GAS_URL = os.environ.get(
    "GAS_WEB_APP_URL",
    "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec")

# 후보 한 종목에서 남길 것. **A1·A2 를 돌리는 데 필요한 최소이자 전부**다.
# 하나라도 빠지면 그 축으로는 반사실 비교를 못 한다.
POOL_FIELDS = ("code", "name", "score", "v1_score", "v2_score", "v3_score",
               "rs_grade", "tajeom_raw", "type", "theme_name", "curr_p")


def _sha8(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def sha8_of_file(path):
    """그 파일(=그날 돌린 코드)의 지문. 정책이 바뀐 구간을 섞지 않기 위해 남긴다."""
    try:
        with open(os.path.abspath(path), "r", encoding="utf-8") as f:
            return _sha8(f.read())
    except Exception:
        return None


def slim_pool(pool):
    """후보 풀에서 동결 대상 필드만 뽑는다. `info` 는 이 값들로 재구성 가능해 뺀다."""
    return [{k: c.get(k) for k in POOL_FIELDS} for c in pool]


def build_bundle(kind, pool=None, prompt=None, response=None, picks=None,
                 extra=None, code_sha=None, now=None):
    """동결 묶음 하나를 만든다. **순수 함수** — 파일도 네트워크도 건드리지 않는다.

    kind: "analyst" | "phase2"
    extra: 종류별 추가 정보(Phase 2 의 원시 일봉·진입/목표/손절 등)
    """
    now = now or datetime.datetime.now(KST)
    body = {
        "freeze_version": FREEZE_VERSION,
        "kind": kind,
        "captured_at": now.isoformat(),
        "trade_date": now.strftime("%Y-%m-%d"),
        "code_sha256_8": code_sha,
        "pool": slim_pool(pool) if pool else None,
        "pool_size": len(pool) if pool else 0,
        "prompt": prompt,
        "response": response,
        "picks": picks,
        "extra": extra,
    }
    return body


def bundle_bytes(bundle):
    """gzip 된 JSON 바이트. mtime=0 으로 고정해 **같은 내용이면 같은 바이트**가 나온다."""
    raw = json.dumps(bundle, ensure_ascii=False, separators=(",", ":"),
                     sort_keys=True).encode("utf-8")
    buf = gzip.compress(raw, mtime=0)
    return buf


def bundle_name(bundle):
    """파일 이름에 날짜·종류·내용 지문을 넣어 **덮어쓰기와 혼동을 막는다.**"""
    digest = _sha8(json.dumps(bundle, ensure_ascii=False, sort_keys=True))
    t = bundle["captured_at"][11:19].replace(":", "")
    return f"freeze_{bundle['trade_date']}_{t}_{bundle['kind']}_{digest}.json.gz"


def write_status(ok, name=None, detail="", path=STATUS_FILE, required=True, **extra):
    """보존 결과를 파일로 남긴다. 워크플로가 이걸 읽어 실패를 드러낸다.

    `required=False` 는 **이 회차가 동결 대상이 아니다**는 뜻이다. 리포트 픽을
    만들지 않는 회차(예: 20시 시간외 브리핑)는 동결할 후보 풀이 없다.
    그때도 파일은 남긴다 — 그래야 **파일이 없다 = 스크립트가 죽었다** 가 된다.
    파일 자체가 없는 것을 '대상 아님'과 같이 취급하면 진짜 사고를 놓친다.
    """
    st = {"ok": bool(ok), "required": bool(required), "file": name, "detail": detail,
          "at": datetime.datetime.now(KST).isoformat()}
    st.update(extra)       # retryable·attempts — 후속 재시도 단계가 읽는다
    with open(path, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    return st


class NoFileId(RuntimeError):
    """응답은 왔는데 파일 id 가 없다 — 저장됐다고 믿을 근거가 없다."""


def upload(gas_url, name, data, timeout=60, *, backoff=(), sleep=time.sleep,
           trace=None, deadline=None, clock=time.monotonic):
    """GAS 웹앱으로 드라이브 업로드. PDF 업로드와 **같은 경로**를 쓴다.

    실패하면 예외를 그대로 올린다 — 조용히 성공한 척하지 않는다.

    `backoff` 가 비어 있으면(기본값) **딱 한 번**만 시도한다. 배지 보관
    (`badge_observations.archive`)이 "애매한 업로드는 자동 재시도하지 않는다" 는 정책으로
    이 함수를 그대로 부르므로 기본값을 바꾸면 그 정책이 조용히 깨진다.
    동결 경로만 `backoff` 를 줘서 재시도한다.

    재시도하는 실패: 네트워크 오류·시간 초과·JSON 이 아닌 응답(오류 페이지·빈 응답)·파일 id 없는 응답.
    그 밖의 예외(코드 결함 등)는 첫 번에 그대로 올라간다.
    `deadline`(clock 기준 절대값)이 있으면 다음 대기가 그 시각을 넘길 때 재시도를 멈춘다.
    `trace` 리스트에는 회차별 결과가 쌓인다 — 몇 번 만에 됐는지·왜 실패했는지 남기려는 것이다.
    """
    import requests
    b64 = base64.b64encode(data).decode("utf-8")
    attempts = len(backoff) + 1
    n = 0
    while True:
        n += 1
        try:
            res = requests.post(gas_url, json={"filename": name, "base64": b64},
                                timeout=timeout).json()
            fid = res.get("id") if isinstance(res, dict) else None
            if not fid:
                raise NoFileId(f"업로드 응답에 파일 id 가 없다: {res}")
            if trace is not None:
                trace.append(f"{n}회차 성공")
            return fid
        except (requests.exceptions.RequestException, ValueError, NoFileId) as e:
            if trace is not None:
                trace.append(f"{n}회차 실패 {type(e).__name__}: {str(e)[:100]}")
            wait = backoff[n - 1] if n <= len(backoff) else None
            if wait is None or (deadline is not None and clock() + wait >= deadline):
                raise
            print(f"   ↻ 업로드 {n}/{attempts}회차 실패({type(e).__name__}) — {wait}초 뒤 다시 시도")
            sleep(wait)


def bundle_is_empty(bundle):
    """묶음에 **실제 내용이 있는가.**

    🚨 [2026-09-08 실측에서 잡힘] 첫 실행이 213바이트짜리 묶음을 올리고
       `ok=true` 로 보고했다. `freeze_rows.append` 가 패치 중 유실돼 목록이
       내내 비어 있었는데, 업로드 자체는 성공해서 **성공처럼 보였다.**
       빈 묶음을 성공으로 세는 것은 실패보다 나쁘다 — 보존됐다고 믿게 만든다.
       그래서 내용 유무를 따로 본다.
    """
    if bundle.get("kind") == "analyst":
        return not bundle.get("pool")
    if bundle.get("kind") == "phase2":
        return not (bundle.get("extra") or {}).get("rows")
    return False


def freeze(gas_url=None, bundle=None, local_dir=None, status_path=STATUS_FILE,
           backoff=UPLOAD_BACKOFF_INLINE, sleep=time.sleep):
    """묶음을 만들어 올리고 결과를 기록한다. **예외를 삼키지 않는다.**

    반환 (ok, name, detail). 호출자는 리포트 발송을 멈추지 않되,
    실패를 **반드시 드러내야** 한다.

    업로드는 일시 오류에 대비해 짧게 재시도한다(`backoff`). 그래도 실패하면 상태 파일에
    `retryable`·`attempts` 를 남긴다 — 리포트를 다 보낸 뒤 워크플로의 별도 단계가
    `retry_pending()` 으로 러너에 남은 사본을 다시 올린다.
    """
    gas_url = DEFAULT_GAS_URL if gas_url is None else gas_url
    name = bundle_name(bundle)
    data = bundle_bytes(bundle)
    if bundle_is_empty(bundle):
        d = ("묶음이 비어 있다 — 올려도 보존되는 내용이 없다. "
             "수집 지점이 끊겼는지 확인하라(2026-09-08 실측에서 이 상태가 있었다)")
        write_status(False, name, d, status_path)
        return False, name, d
    if local_dir:                      # 러너에 남겨 두면 같은 잡 안에서는 볼 수 있다
        os.makedirs(local_dir, exist_ok=True)
        with open(os.path.join(local_dir, name), "wb") as f:
            f.write(data)
    if not gas_url:
        d = "GAS_WEB_APP_URL 이 없어 업로드하지 않았다 — 잡이 끝나면 사라진다"
        write_status(False, name, d, status_path)
        return False, name, d
    trace = []
    try:
        fid = upload(gas_url, name, data, backoff=backoff, sleep=sleep, trace=trace)
    except Exception as e:
        tried = len(trace)
        d = f"업로드 실패: {str(e)[:400]}" + (f" — {tried}회 시도" if tried > 1 else "")
        # 사본이 러너에 있어야 나중에 다시 올릴 수 있다
        write_status(False, name, d, status_path, retryable=bool(local_dir), attempts=tried)
        return False, name, d
    tried = len(trace)
    d = (f"드라이브 저장 완료 id={fid} ({len(data):,}바이트)"
         + (f" — {tried}회째 시도에서 성공" if tried > 1 else ""))
    write_status(True, name, d, status_path, attempts=tried)
    return True, name, d


def retry_pending(status_path=STATUS_FILE, local_dir=LOCAL_DIR, gas_url=None,
                  backoff=UPLOAD_BACKOFF_LATE, budget_seconds=LATE_BUDGET_SECONDS,
                  sleep=time.sleep, clock=time.monotonic):
    """동결이 **업로드 실패** 로 끝난 회차만, 러너에 남은 사본을 다시 올린다.

    리포트를 다 보낸 뒤 워크플로의 별도 단계에서 부른다. 호출 중의 짧은 재시도로는
    못 넘긴 몇 분짜리 일시 오류를 넘기려는 것이다(10/2 는 3분 뒤 창구가 정상이었다).

    반환 (행동, 사유) — 행동은 "none"(할 일 없음) · "recovered"(보존됨) · "failed"(끝내 실패).
    다시 올리지 않는 경우: 상태 파일이 없거나, 동결 대상이 아니거나, 이미 성공했거나,
    업로드 실패가 아니거나(빈 묶음·코드 예외 등), 사본이 없거나, 사본이 이름과 맞지 않을 때.
    사본이 이름과 맞는지 보는 이유 — 이름은 내용 해시를 품고 있어 엉뚱한 파일을 올리지 않게 한다.
    """
    try:
        with open(status_path, encoding="utf-8") as f:
            st = json.load(f)
    except (OSError, ValueError):
        return "none", "상태 파일을 읽지 못했다 — 확인 단계가 잡는다"
    if not st.get("required", True):
        return "none", "이 회차는 동결 대상이 아니다"
    if st.get("ok"):
        return "none", "이미 보존됐다"
    if not st.get("retryable"):
        return "none", f"재시도 대상이 아니다 — {str(st.get('detail'))[:120]}"
    name = st.get("file")
    path = os.path.join(local_dir, name) if name else None
    if not path or not os.path.isfile(path):
        return "none", "러너에 사본이 없다 — 다시 올릴 수 없다"
    with open(path, "rb") as f:
        data = f.read()
    try:
        same = bundle_name(json.loads(gzip.decompress(data).decode("utf-8"))) == name
    except Exception:                                    # noqa: BLE001 — 깨진 사본은 올리지 않는다
        same = False
    if not same:
        return "none", "사본이 이름과 맞지 않는다 — 올리지 않는다"
    gas_url = DEFAULT_GAS_URL if gas_url is None else gas_url
    if not gas_url:
        return "none", "GAS URL 이 없다 — 다시 올릴 수 없다"
    before = int(st.get("attempts") or 0)
    trace = []
    try:
        fid = upload(gas_url, name, data, backoff=backoff, sleep=sleep, trace=trace,
                     deadline=clock() + budget_seconds, clock=clock)
    except Exception as e:
        total = before + len(trace)
        d = f"업로드 실패: {str(e)[:400]} — 후속 재시도 포함 총 {total}회 시도"
        write_status(False, name, d, status_path, retryable=True, attempts=total)
        return "failed", d
    total = before + len(trace)
    d = (f"드라이브 저장 완료 id={fid} ({len(data):,}바이트) — 후속 재시도로 보존(총 {total}회째 시도). "
         f"앞선 실패: {str(st.get('detail'))[:120]}")
    write_status(True, name, d, status_path, attempts=total)
    return "recovered", d


def self_test():
    ok = True

    def chk(label, cond, note=""):
        nonlocal ok
        ok = ok and bool(cond)
        print(f"  {'✅' if cond else '❌'} {label}" + (f"   {note}" if note else ""))

    print("🧪 실행 입력 동결")
    pool = [{"code": "000660", "name": "SK하이닉스", "score": 88, "v1_score": 88,
             "v2_score": 71, "v3_score": 40, "rs_grade": 92,
             "tajeom_raw": "🚀 대장 · 당일단타", "type": "NORMAL",
             "theme_name": "반도체", "curr_p": 210000, "info": "버려도 되는 필드"}]
    now = datetime.datetime(2026, 9, 8, 15, 0, 5, tzinfo=KST)
    b = build_bundle("analyst", pool=pool, prompt="P", response="R",
                     picks={"short_term_code": "000660"}, code_sha="abcd1234", now=now)

    chk("A1·A2 에 필요한 필드가 전부 남는다",
        all(k in b["pool"][0] for k in POOL_FIELDS), f"{sorted(b['pool'][0])}")
    chk("점수가 값 그대로 보존된다",
        (b["pool"][0]["v1_score"], b["pool"][0]["rs_grade"]) == (88, 92))
    chk("재구성 가능한 info 는 싣지 않는다", "info" not in b["pool"][0])
    chk("프롬프트·응답 원문이 남는다 (A2 의 전제)",
        (b["prompt"], b["response"]) == ("P", "R"))
    chk("코드 지문이 붙는다", b["code_sha256_8"] == "abcd1234")
    chk("파일 지문은 8자리 16진수", len(sha8_of_file(__file__)) == 8,
        sha8_of_file(__file__))
    chk("없는 파일이면 None (동결이 그것 때문에 죽지 않는다)",
        sha8_of_file("/없는/경로") is None)

    d1, d2 = bundle_bytes(b), bundle_bytes(build_bundle(
        "analyst", pool=pool, prompt="P", response="R",
        picks={"short_term_code": "000660"}, code_sha="abcd1234", now=now))
    chk("같은 내용이면 같은 바이트 (gzip mtime 고정)", d1 == d2, f"{len(d1)}바이트")
    b2 = build_bundle("analyst", pool=pool, prompt="P2", response="R",
                      picks=None, code_sha="abcd1234", now=now)
    chk("내용이 다르면 이름도 다르다", bundle_name(b) != bundle_name(b2))
    chk("이름에 날짜·종류가 보인다",
        "2026-09-08" in bundle_name(b) and "analyst" in bundle_name(b),
        bundle_name(b))
    chk("gzip 을 풀면 원본이 그대로 나온다",
        json.loads(gzip.decompress(d1).decode("utf-8"))["pool"][0]["code"] == "000660")

    # Phase 2 — 원시 일봉과 그때의 진입/목표/손절
    bars = [{"date": "2026-09-01", "open": 100, "high": 101, "low": 99, "close": 100}]
    p2 = build_bundle("phase2", extra={"rows": [{"code": "018470", "entry": "2026-08-04",
                                                 "base": 100, "target": 110, "stop": 92,
                                                 "bars": bars}]}, now=now)
    chk("Phase 2 는 원시 일봉과 선 가격을 같이 남긴다",
        p2["extra"]["rows"][0]["bars"] == bars
        and p2["extra"]["rows"][0]["stop"] == 92)
    chk("종류가 파일 이름에 들어간다", "phase2" in bundle_name(p2))

    # 보존 실패를 성공처럼 넘기지 않는다
    import tempfile, shutil
    tmp = tempfile.mkdtemp(prefix="freeze_test_")
    try:
        sp = os.path.join(tmp, "st.json")
        okf, nm, det = freeze("", b, local_dir=tmp, status_path=sp)
        chk("GAS URL 이 없으면 **실패로 기록**한다(조용히 넘어가지 않음)", okf is False)
        chk("그래도 로컬 사본은 남긴다", os.path.exists(os.path.join(tmp, nm)))
        st = json.load(open(sp, encoding="utf-8"))
        chk("상태 파일에 ok=false 가 적힌다", st["ok"] is False, st["detail"])
        okf2, nm2, det2 = freeze("http://127.0.0.1:9/none", b,
                                 local_dir=tmp, status_path=sp, backoff=())
        chk("업로드가 터져도 예외로 죽지 않고 실패를 기록한다", okf2 is False)
        # 🚨 빈 묶음을 성공으로 세지 않는다 — 실측에서 실제로 있었던 상태다
        empty_p2 = build_bundle("phase2", extra={"horizon": 20, "rows": []}, now=now)
        chk("빈 phase2 묶음은 비어 있다고 판정한다", bundle_is_empty(empty_p2))
        eok, enm, edet = freeze("https://example.invalid", empty_p2,
                                local_dir=tmp, status_path=sp)
        chk("빈 묶음은 업로드 시도조차 하지 않고 실패로 기록한다",
            eok is False and "비어" in edet, edet)
        chk("내용이 있으면 통과한다", bundle_is_empty(b) is False)
        chk("phase2 도 행이 있으면 통과", bundle_is_empty(p2) is False)
        chk("그때 상태 파일도 ok=false", json.load(open(sp, encoding="utf-8"))["ok"] is False)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("🧪 required — '대상 아님'과 '실패'를 섞지 않는다 (2026-09-11)")
    import tempfile, os as _os
    _d = tempfile.mkdtemp()
    _p = _os.path.join(_d, "s.json")
    st = write_status(True, None, "20시 브리핑 회차", path=_p, required=False)
    chk("required=False 가 파일에 남는다", st["required"] is False)
    chk("대상 아님도 ok=True 다 (실패가 아니다)", st["ok"] is True)
    st2 = write_status(True, "b.tar.gz", "보존 완료", path=_p)
    chk("기본값은 required=True — 기존 호출은 그대로", st2["required"] is True)
    chk("나중 쓰기가 앞 상태를 덮어쓴다",
        json.load(open(_p, encoding="utf-8"))["file"] == "b.tar.gz")
    st3 = write_status(False, None, "업로드 실패", path=_p)
    chk("실패는 ok=False · required=True 로 남는다",
        st3["ok"] is False and st3["required"] is True)

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    if "--retry-pending" in sys.argv:
        # 워크플로 단계용. 결과는 freeze_status.json 에 반영되고, 이어지는 '동결 확인' 단계가 판정한다.
        # 이 단계는 실패해도 잡을 죽이지 않는다 — 어떤 경우에도 0 으로 끝낸다.
        try:
            action, why = retry_pending()
        except Exception as e:                           # noqa: BLE001
            action, why = "none", f"재시도 단계 자체가 예외 {type(e).__name__}: {e}"
        print({"recovered": "🧊 후속 재시도로 보존했다", "failed": "❌ 후속 재시도도 실패했다",
               "none": "ℹ️ 후속 재시도 대상 아님"}[action] + f" — {why}")
        sys.exit(0)
    sys.exit(self_test())
