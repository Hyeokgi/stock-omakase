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

KST = datetime.timezone(datetime.timedelta(hours=9))
FREEZE_VERSION = "run-freeze-v1"
STATUS_FILE = "freeze_status.json"

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


def write_status(ok, name=None, detail="", path=STATUS_FILE, required=True):
    """보존 결과를 파일로 남긴다. 워크플로가 이걸 읽어 실패를 드러낸다.

    `required=False` 는 **이 회차가 동결 대상이 아니다**는 뜻이다. 리포트 픽을
    만들지 않는 회차(예: 20시 시간외 브리핑)는 동결할 후보 풀이 없다.
    그때도 파일은 남긴다 — 그래야 **파일이 없다 = 스크립트가 죽었다** 가 된다.
    파일 자체가 없는 것을 '대상 아님'과 같이 취급하면 진짜 사고를 놓친다.
    """
    st = {"ok": bool(ok), "required": bool(required), "file": name, "detail": detail,
          "at": datetime.datetime.now(KST).isoformat()}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    return st


def upload(gas_url, name, data, timeout=60):
    """GAS 웹앱으로 드라이브 업로드. PDF 업로드와 **같은 경로**를 쓴다.

    실패하면 예외를 그대로 올린다 — 조용히 성공한 척하지 않는다.
    """
    import requests
    b64 = base64.b64encode(data).decode("utf-8")
    res = requests.post(gas_url, json={"filename": name, "base64": b64},
                        timeout=timeout).json()
    fid = res.get("id")
    if not fid:
        raise RuntimeError(f"업로드 응답에 파일 id 가 없다: {res}")
    return fid


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


def freeze(gas_url=None, bundle=None, local_dir=None, status_path=STATUS_FILE):
    """묶음을 만들어 올리고 결과를 기록한다. **예외를 삼키지 않는다.**

    반환 (ok, name, detail). 호출자는 리포트 발송을 멈추지 않되,
    실패를 **반드시 드러내야** 한다.
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
    try:
        fid = upload(gas_url, name, data)
    except Exception as e:
        d = f"업로드 실패: {e}"
        write_status(False, name, d, status_path)
        return False, name, d
    d = f"드라이브 저장 완료 id={fid} ({len(data):,}바이트)"
    write_status(True, name, d, status_path)
    return True, name, d


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
                                 local_dir=tmp, status_path=sp)
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
    sys.exit(self_test())
