import os, requests, datetime
from bs4 import BeautifulSoup
from google import genai

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
KST = datetime.timezone(datetime.timedelta(hours=9))

def get_us_market_summary():
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # 네이버 금융 글로벌 홈에서 밤사이 미 증시 요약 스크래핑
        url = "https://finance.naver.com/world/"
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        
        # 나스닥, S&P500 등 지수 텍스트 추출
        indices = soup.select('.world_index .idx_tit, .world_index .idx_val, .world_index .idx_fluc')
        market_data = " ".join([i.text.strip() for i in indices[:15]])
        
        # 밤사이 글로벌 주요 뉴스 헤드라인 추출
        news_items = soup.select('.news_area .tit')
        news_data = "\n".join([f"- {n.text.strip()}" for n in news_items[:5]])
        
        return market_data, news_data
    except Exception as e:
        return "미 증시 데이터 수집 지연", f"뉴스 수집 지연: {e}"

def generate_morning_briefing():
    client = genai.Client(api_key=GEMINI_API_KEY)
    market_data, news_data = get_us_market_summary()
    
    # 💡 [핵심] 아침 8시 실전 트레이더를 위한 '모닝 마스터 프롬프트'
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로 애널리스트야.
아래의 밤사이 글로벌 마감 데이터와 뉴스를 분석해서, 오늘 아침 9시 장이 열리기 전 트레이더가 반드시 알아야 할 '모닝 브리핑'을 작성해라.

[밤사이 데이터]
미 증시 요약: {market_data}
글로벌 뉴스 헤드라인: {news_data}

[작성 지침]
1. 간결하고 묵직한 어조: 쓸데없는 인사말이나 이모지는 최소화하고, 전장의 장수에게 보고하듯 핵심만 찔러라.
2. 3단 구성:
   - 🌎 [글로벌 매크로 요약]: 간밤에 나스닥과 S&P가 왜 오르고 내렸는지, 유가나 지정학적 리스크(예: 이란/미국 등)의 핵심 동인이 무엇인지 3줄로 요약.
   - 🇰🇷 [국내 증시 투영 (Impact)]: 이 매크로 흐름이 오늘 코스피/코스닥의 어느 섹터(반도체, 방산, 에너지 등)에 호재/악재로 작용할지 논리적으로 연결.
   - 🎯 [오늘의 액션 플랜]: "오늘 갭상승이 예상되는 섹터는 추격을 자제하라", "낙폭과대 반도체를 시가에 노려라" 등 구체적인 오전장(9시~10시) 행동 지침 제시.
"""
    
    response = client.models.generate_content(model='gemini-2.5-pro', contents=prompt)
    return response.text

if __name__ == "__main__":
    briefing_text = generate_morning_briefing()
    today_str = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    
    final_msg = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    # 텔레그램으로 즉시 텍스트 발송 (PDF 변환 없이 아침 출근길에 바로 읽기 좋게 텍스트로 쏩니다)
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={'chat_id': TELEGRAM_CHAT_ID, 'text': final_msg, 'parse_mode': 'Markdown'}
    )
    print("✅ 모닝 브리핑 텔레그램 발송 완료!")
