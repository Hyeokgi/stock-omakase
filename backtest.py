import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib3
import xml.etree.ElementTree as ET

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'})

def get_top_volume_stocks():
    print("▶️ 최근 거래대금 상위 200개 종목 수집 중...")
    stocks = []
    for sosok in ['0', '1']: # 0: KOSPI, 1: KOSDAQ
        url = f"https://finance.naver.com/sise/sise_quant_high.naver?sosok={sosok}"
        res = session.get(url, verify=False)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        for a in soup.select('table.type_2 a.tltle')[:100]:
            code = a['href'].split('code=')[-1]
            name = a.text.strip()
            stocks.append((name, code))
        time.sleep(0.5)
    return stocks

def run_backtest():
    stocks = get_top_volume_stocks()
    
    ss_results = {'count': 0, 'win_open': 0, 'win_high': 0, 'sum_open_yield': 0, 'sum_high_yield': 0}
    s_results = {'count': 0, 'win_open': 0, 'win_high': 0, 'sum_open_yield': 0, 'sum_high_yield': 0}
    
    print(f"▶️ 총 {len(stocks)}개 종목 최근 60일 차트 딥 리서치 시작...")
    
    for idx, (name, code) in enumerate(stocks, 1):
        if idx % 20 == 0: print(f"... {idx}개 종목 분석 완료")
        try:
            url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
            root = ET.fromstring(session.get(url, verify=False, timeout=3).text)
            
            items = root.findall(".//item")
            if len(items) < 30: continue
            
            df = pd.DataFrame([item.get("data").split("|") for item in items], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df = df.astype({'open': int, 'high': int, 'low': int, 'close': int, 'volume': int})
            
            # T일(과거 특정일) 기준으로 조건 검사 후 T+1일(다음날) 결과 확인
            for i in range(20, len(df) - 1):
                window = df.iloc[i-20:i+1].copy()
                next_day = df.iloc[i+1]
                
                t_close = window['close'].iloc[-1]
                t_open = window['open'].iloc[-1]
                t_high = window['high'].iloc[-1]
                t_vol = window['volume'].iloc[-1]
                
                prev_close = window['close'].iloc[-2]
                change_rate = (t_close - prev_close) / prev_close if prev_close > 0 else 0
                
                ma5 = window['close'].tail(5).mean()
                ma20 = window['close'].tail(20).mean()
                std20 = window['close'].tail(20).std()
                upper_band = ma20 + (std20 * 2)
                band_width = (upper_band - (ma20 - (std20 * 2))) / ma20 if ma20 > 0 else 0
                
                avg_vol_10 = window['volume'].tail(11).head(10).mean()
                vol_ratio = (t_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
                yest_vol_ratio = (window['volume'].iloc[-2] / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
                
                high_60d = window['high'].max()
                is_near_high = t_close >= (high_60d * 0.90)
                
                body_top = max(t_close, t_open)
                upper_shadow = t_high - body_top
                real_body = body_top - min(t_close, t_open)
                upper_shadow_ratio = upper_shadow / t_close if t_close > 0 else 0
                is_long_shadow = (upper_shadow_ratio >= 0.05) or (upper_shadow_ratio >= 0.025 and upper_shadow > real_body * 1.5)
                
                # 🎯 [SS급] 전고점 돌파 조건 검사
                is_ss_breakout = vol_ratio >= 150 and change_rate >= 0.05 and not is_long_shadow and is_near_high
                
                # 🎯 [S급] 도지 눌림목 조건 검사
                is_doji = abs(change_rate) <= 0.03 and vol_ratio <= 60 and yest_vol_ratio >= 120 and t_close >= ma5
                
                if is_ss_breakout or is_doji:
                    # 다음 날 수익률 계산 (시가 수익률, 고가 수익률)
                    next_open_yield = (next_day['open'] - t_close) / t_close
                    next_high_yield = (next_day['high'] - t_close) / t_close
                    
                    target_dict = ss_results if is_ss_breakout else s_results
                    target_dict['count'] += 1
                    target_dict['sum_open_yield'] += next_open_yield
                    target_dict['sum_high_yield'] += next_high_yield
                    if next_open_yield > 0: target_dict['win_open'] += 1
                    if next_high_yield >= 0.03: target_dict['win_high'] += 1 # 다음날 3% 이상 고가를 찍을 확률
                    
        except Exception as e:
            continue

    print("\n" + "="*50)
    print("📈 [딥 리서치 결과] 최근 60일 종가베팅 백테스트")
    print("="*50)
    
    for name, res in [("👑 [SS급] 전고점 돌파", ss_results), ("🎯 [S급] 도지 눌림목", s_results)]:
        cnt = res['count']
        if cnt == 0: continue
        open_win_rate = (res['win_open'] / cnt) * 100
        high_win_rate = (res['win_high'] / cnt) * 100
        avg_open = (res['sum_open_yield'] / cnt) * 100
        avg_high = (res['sum_high_yield'] / cnt) * 100
        
        print(f"\n{name} (포착 횟수: {cnt}회)")
        print(f" - 다음날 갭상승 확률: {open_win_rate:.1f}% (평균 시가 갭: {avg_open:.2f}%)")
        print(f" - 다음날 장중 +3% 이상 수익 줄 확률: {high_win_rate:.1f}% (평균 최고 수익: {avg_high:.2f}%)")

if __name__ == "__main__":
    run_backtest()
