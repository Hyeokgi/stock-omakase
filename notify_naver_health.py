"""Consume complete structured health results; never infer success from absent log lines."""
import json
import os
from pathlib import Path
import sys
from naver_health_policy import evaluate


def notification(payload, exit_code, run_id):
    if not isinstance(payload, dict) or payload.get("schema") != "naver-health-v2" or payload.get("run_id") != run_id:
        raise ValueError("진단 결과 누락/버전/실행 ID 불일치")
    result = evaluate(payload["results"])
    expected_exit = "1" if result["alerts"] else "0"
    if not result["complete"] or exit_code != expected_exit:
        raise ValueError("진단 미완료 또는 종료코드 불일치")
    if not result["alerts"]:
        return None
    fatal = any(a["severity"] == "치명적" for a in result["alerts"])
    lines = ["🚨 [네이버 의존 진단] 실제 원천 장애" if fatal else "⚠️ [네이버 의존 진단] 데이터 품질/기능 점검 필요"]
    lines.extend(f"- {a['name']}: {a['detail']}" for a in result["alerts"])
    lines.append("API 표본 진단입니다. 운영 시트 갱신 여부는 별도 확인 대상입니다.")
    return "\n".join(lines)


def main():
    try:
        payload = json.loads(Path("health_report.json").read_text(encoding="utf-8"))
        msg = notification(payload, os.environ.get("EXIT_CODE", ""), os.environ.get("GITHUB_RUN_ID", ""))
    except Exception as error:
        msg = f"🚨 [네이버 의존 진단] 진단 실행/결과 검증 실패: {type(error).__name__}"
    if msg is None:
        print("원천 검사 통과. 구경로 종료/대체 성공은 artifact에 보존; 반복 알림 생략. 시트 미검증.")
        return 0
    msg = (msg + "\n" + os.environ.get("RUN_URL", ""))[:3900]
    print(msg)
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat:
        import requests
        response = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                 data={"chat_id": chat, "text": msg}, timeout=15)
        response.raise_for_status()
        if response.json().get("ok") is not True:
            raise RuntimeError("Telegram rejected notification")
    else:
        print("Telegram credentials absent; failing job with log only.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
