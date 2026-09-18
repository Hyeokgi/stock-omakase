# -*- coding: utf-8 -*-
"""
피처 실패 계수기 — "조용히 틀림" 을 "시끄럽게 모름" 으로 바꾼다.

2026-09-18 사용자 지시 ⑤ + 5번(silent exception 12곳).

두 지시가 사실 같은 것을 요구한다.

  ⑤ "V1/V2/V3 조용한 파싱오류 0 을 각각의 생산 telemetry 로 증명한다"
  5 "핵심 feature 실패는 잘못된 기본값 금지. None/ERROR/reason 으로 명시한다"

즉 실패를 **세고 사유를 남기면** 둘 다 충족된다. 그래서 계수기 하나로 합친다.

분류(사용자 지시 5번의 4갈래)
-----------------------------
  CRITICAL  핵심 Feature·가격·정책·후보선정 — 잘못된 기본값 금지.
            값을 쓰지 않고(None) 세고 사유를 남긴다. **Gate 를 떨어뜨린다.**
  OBSERVE   관측 저장 실패 — 생산 스캐너는 살리되 **Stability cycle 은 실패**시킨다.
  DISPLAY   UI·표시·부가 설명 — warning 허용. Gate 에 영향 없다.
  RESEARCH  연구 전용 — 생산은 유지하되 **해당 연구 표본은 fail-closed.**

왜 `raise` 로 바꾸지 않는가
---------------------------
12곳을 일괄 raise 로 바꾸면 종목 하나가 이상할 때 그날 수집 전체를 잃는다.
그건 "조용히 틀림" 을 "요란하게 아무것도 못함" 으로 바꾸는 것일 뿐이다.
우리가 원하는 것은 **그 값만 쓰지 않고, 그 사실이 보이는 것**이다.
"""
import collections

CRITICAL = "CRITICAL"
OBSERVE = "OBSERVE"
DISPLAY = "DISPLAY"
RESEARCH = "RESEARCH"
CLASSES = (CRITICAL, OBSERVE, DISPLAY, RESEARCH)

# Gate 를 떨어뜨리는 분류
GATE_BLOCKING = (CRITICAL, OBSERVE)


class Telemetry:
    """실패를 센다. 던지지 않는다. 지우지 않는다."""

    def __init__(self):
        self.counts = collections.Counter()      # (feature, klass) -> n
        self.reasons = collections.defaultdict(collections.Counter)  # feature -> reason -> n
        self.samples = collections.defaultdict(list)                 # feature -> [코드...]

    def note(self, feature, reason, klass=CRITICAL, code=""):
        """실패 하나. 반환값은 **항상 None** — 호출부가 값 대신 쓰라는 뜻이다."""
        if klass not in CLASSES:
            klass = CRITICAL                     # 모르면 가장 엄한 쪽
        self.counts[(feature, klass)] += 1
        self.reasons[feature][reason] += 1
        if code and len(self.samples[feature]) < 5:
            self.samples[feature].append(str(code))
        return None

    def errors(self, feature, classes=GATE_BLOCKING):
        return sum(n for (f, k), n in self.counts.items()
                   if f == feature and k in classes)

    def total(self, classes=GATE_BLOCKING):
        return sum(n for (_, k), n in self.counts.items() if k in classes)

    def snapshot(self):
        """영수증에 실을 형태. 숫자와 사유만 — 판정하지 않는다."""
        feats = sorted({f for f, _ in self.counts})
        return {
            "by_feature": {f: {k: self.counts[(f, k)] for k in CLASSES
                               if self.counts[(f, k)]} for f in feats},
            "reasons": {f: dict(self.reasons[f]) for f in feats},
            "samples": {f: list(self.samples[f]) for f in feats},
            "gate_blocking_total": self.total(),
        }

    def render(self):
        if not self.counts:
            return "📊 [피처 실패] 없음"
        parts = []
        for f in sorted({x for x, _ in self.counts}):
            worst = ", ".join(f"{r}×{n}" for r, n in self.reasons[f].most_common(3))
            parts.append(f"{f}={self.errors(f)}({worst})")
        return f"📊 [피처 실패] {' · '.join(parts)} · 차단합계 {self.total()}"


def _selftest():
    ok = 0

    def chk(name, cond, extra=""):
        nonlocal ok
        assert cond, f"{name} {extra}"
        ok += 1
        print(f"  ✅ {name}{('   ' + str(extra)) if extra else ''}")

    print("🧪 피처 실패 계수기")
    t = Telemetry()
    chk("아무 일 없으면 0", t.total() == 0 and t.errors("v1") == 0)
    chk("사유 없는 성공 보고를 만들지 않는다", "없음" in t.render())

    chk("note 는 항상 None 을 준다 — 값 대신 쓰라는 뜻",
        t.note("v1", "이격률 파싱", code="005930") is None)
    chk("센다", t.errors("v1") == 1)
    chk("사유가 남는다", t.reasons["v1"]["이격률 파싱"] == 1)
    chk("표본 코드가 남는다", t.samples["v1"] == ["005930"])

    for i in range(10):
        t.note("v1", "이격률 파싱", code=f"00000{i}")
    chk("표본은 5개까지만(로그를 덮지 않는다)", len(t.samples["v1"]) == 5)
    chk("세는 것은 계속 센다", t.errors("v1") == 11)

    t.note("theme", "테마 대금 파싱", klass=DISPLAY)
    chk("DISPLAY 는 Gate 를 막지 않는다", t.errors("theme") == 0, t.counts)
    chk("그래도 기록은 된다", t.counts[("theme", DISPLAY)] == 1)

    t.note("store", "feature_store 쓰기", klass=OBSERVE)
    chk("OBSERVE 는 Gate 를 막는다", t.errors("store") == 1)

    t.note("pool", "rank_pool 기록", klass=RESEARCH)
    chk("RESEARCH 는 Gate 를 막지 않는다(연구 표본만 fail-closed)", t.errors("pool") == 0)

    chk("모르는 분류는 가장 엄한 쪽으로", Telemetry().note("x", "y", klass="이상함") is None)
    t2 = Telemetry(); t2.note("x", "y", klass="이상함")
    chk("그리고 CRITICAL 로 센다", t2.errors("x") == 1)

    snap = t.snapshot()
    chk("스냅샷에 피처별 수가 있다", snap["by_feature"]["v1"][CRITICAL] == 11)
    chk("스냅샷에 사유가 있다", snap["reasons"]["v1"]["이격률 파싱"] == 11)
    chk("차단 합계가 맞는다", snap["gate_blocking_total"] == 12, snap["gate_blocking_total"])
    chk("렌더가 사유를 보여준다", "이격률 파싱" in t.render())

    chk("분류는 넷", len(CLASSES) == 4)
    chk("Gate 차단은 CRITICAL·OBSERVE 둘", set(GATE_BLOCKING) == {CRITICAL, OBSERVE})
    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
