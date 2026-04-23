import requests
import datetime
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

KST = datetime.timezone(datetime.timedelta(hours=9))

def get_daum_after_hours_price(code):
    print(f"\n📡 [Daum] {code} 시간외 단일가 찔러보는 중...")
    url = f"https://finance.daum.net/api/quotes/A{code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': f'https://finance.daum.net/quotes/A{code}',
        'Accept': 'application/json, text/javascript, */*; q=0.01'
    }
    try:
        res = requests.get(url, headers=headers, verify=False, timeout=5)
        if res.status_code == 200:
            data = res.json()
            after_price = data.get('timeExtraPrice') or data.get('overTimePrice') or 0
            return f"✅ 성공: {after_price}원"
        else:
            return f"❌ 실패: 상태코드 {res.status_code} - {res.text[:100]}"
    except Exception as e:
        return f"🚨 에러 발생: {e}"

def get_nxt_official_price(code):
    print(f"\n📡 [NXT] {code} 야간거래 찔러보는 중...")
    
    now = datetime.datetime.now(KST)
    print(f"현재 KST 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if now.hour < 9:
        target_date = (now - datetime.timedelta(days=1)).strftime('%Y%m%d')
        print(f"새벽이므로 '어제' 날짜({target_date})로 요청합니다.")
    else:
        target_date = now.strftime('%Y%m%d')
        print(f"'오늘' 날짜({target_date})로 요청합니다.")

    url = "https://www.nextrade.co.kr/api/transactionStatus/selectTransactionStatusList.do"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': 'https://www.nextrade.co.kr/menu/transactionStatusMain/menuList.do',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01'
    }
    payload = {
        'srchMktId': 'ALL',
        'srchSecGrpId': 'ST',
        'srchTrdDate': target_date 
    }
    
    try:
        res = requests.post(url, headers=headers, data=payload, verify=False, timeout=5)
        if res.status_code == 200:
            data_list = res.json().get('data', [])
            for item in data_list:
                if str(item.get('isuCd', '')).endswith(code) or str(item.get('isuSrtCd', '')).endswith(code):
                    nxt_price = item.get('clpr') or item.get('trdClpr') or 0
                    return f"✅ 성공: {nxt_price}원"
            return "⚠️ 통신은 성공했으나 응답 데이터 목록에 해당 종목이 없습니다."
        else:
            return f"❌ 실패: 상태코드 {res.status_code} - {res.text[:100]}"
    except Exception as e:
        return f"🚨 에러 발생: {e}"

if __name__ == "__main__":
    print("🚀 [진단 테스트 시작] 삼성전자(005930) 데이터 호출")
    print(get_daum_after_hours_price("005930"))
    print("-" * 50)
    print(get_nxt_official_price("005930"))
    print("\n✅ 테스트 완료")
