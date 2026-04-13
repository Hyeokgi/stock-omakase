import os, requests, datetime, time
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
   - 🌎 [글로벌 매크로 요약]: 간밤에 나스닥과 S&P가 왜 오르고 내렸는지, 유가나 지정학적 리스크의 핵심 동인이 무엇인지 3줄로 요약.
   - 🇰🇷 [국내 증시 투영 (Impact)]: 이 매크로 흐름이 오늘 코스피/코스닥의 어느 섹터(반도체, 방산, 에너지 등)에 호재/악재로 작용할지 논리적으로 연결.
   - 🎯 [오늘의 액션 플랜]: "오늘 갭상승이 예상되는 섹터는 추격을 자제하라", "낙폭과대 반도체를 시가에 노려라" 등 구체적인 오전장(9시~10시) 행동 지침 제시.
"""
    
    # 💡 [핵심 패치] 503 에러 튕김 방지! 죽지 않고 10번까지 재시도하는 불사조 로직
    for i in range(10):
        try:
            response = client.models.generate_content(model='gemini-2.5-pro', contents=prompt)
            return response.text
        except Exception as e:
            err_str = str(e).lower()
            print(f"⚠️ [돌파 시도 {i+1}/10] 서버 응답: {e}")
            
            # 503(과부하) 또는 429(트래픽 초과) 시, 대기 시간을 점진적으로 늘리며 재시도
            if "503" in err_str or "unavailable" in err_str or "429" in err_str or "quota" in err_str:
                wait_time = 30 * (i + 1)
                print(f"🚨 서버 혼잡. {wait_time}초 동안 숨을 고른 후 다시 문을 두드립니다...")
                time.sleep(wait_time)
            else: 
                raise e # 진짜 치명적 에러면 시스템 중지
                
    raise Exception("❌ 10번의 재시도에도 구글 서버가 문을 열어주지 않습니다. 나중에 다시 시도하세요.")

if __name__ == "__main__":
    print("🌅 [HYEOKS 모닝 브리핑] 엔진 가동 중...")
    briefing_text = generate_morning_briefing()
    today_str = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    
    final_msg = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    # 텔레그램으로 즉시 텍스트 발송
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={'chat_id': TELEGRAM_CHAT_ID, 'text': final_msg, 'parse_mode': 'Markdown'}
    )
    print("✅ 모닝 브리핑 텔레그램 발송 완료!")
