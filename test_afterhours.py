import requests

code = "005930" # 삼성전자
url = f"https://m.stock.naver.com/api/stock/{code}/basic" # 또는 integrationInfo
headers = {
    # 네이버가 봇으로 인식하지 못하도록 모바일 브라우저 User-Agent 필수 포함
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15"
}

res = requests.get(url, headers=headers)
data = res.json()

# JSON 구조 안에서 NXT 관련 필드 찾기 (실제 응답 구조에 맞게 수정 필요)
# 예: data.get('nxtClosePrice') 또는 data.get('timeExtraPrice') 등
