import requests
import urllib3
urllib3.disable_warnings()

print("=========================================")
print("🚀 [야간 생존 확인] 네이버 딥다이브 테스트")
print("=========================================")

def test_naver_deep_dive(code, name):
    print(f"\n📡 [{name} ({code})] 데이터 탐지 시작...")
    session = requests.Session()
    # 💡 모바일 브라우저로 완벽하게 위장하는 핵심 헤더
    session.headers.update({'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15'})
    
    print("👉 1타겟: integrationInfo (야간 유지력 가장 좋음)")
    try:
        res = session.get(f"https://m.stock.naver.com/api/stock/{code}/integrationInfo", verify=False, timeout=3)
        data = res.json()
        nxt = data.get('nxtPrice') or data.get('nxtClosePrice')
        after = data.get('afterClosePrice') or data.get('timeExtraClosePrice') or data.get('timeExtraPrice')
        print(f"  [결과] NXT: {nxt} / 시간외: {after}")
    except Exception as e:
        print(f"  [에러] {e}")

    print("👉 2타겟: basic (전통적인 방식)")
    try:
        res = session.get(f"https://m.stock.naver.com/api/stock/{code}/basic", verify=False, timeout=3)
        data = res.json()
        nxt = data.get('nxtClosePrice')
        ext = data.get('timeExtraClosePrice')
        print(f"  [결과] NXT: {nxt} / 시간외: {ext}")
    except Exception as e:
        print(f"  [에러] {e}")

# 삼성전자(우량주-NXT활발)와 대한광통신(일반주) 두 개를 테스트합니다.
test_naver_deep_dive("005930", "삼성전자")
test_naver_deep_dive("010170", "대한광통신")

print("\n✅ 테스트가 완료되었습니다.")
