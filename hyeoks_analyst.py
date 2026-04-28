import os
import pandas as pd
import google.generativeai as genai
import time
import json
import re

# ==========================================
# [1단계] DB_스캐너 "대기중" 텍스트 실전 브리핑으로 교체
# ==========================================
def generate_short_briefings():
    DB_SCANNER_PATH = "주식정리 - DB_스캐너.csv"
    print("▶ DB_스캐너 종목 대상 실전 매매 브리핑 생성을 시작합니다...")
    
    try:
        df_scanner = pd.read_csv(DB_SCANNER_PATH, encoding='utf-8-sig')
    except Exception as e:
        print(f"DB_스캐너 로드 실패: {e}")
        return

    # 브리핑 전용 프롬프트 (기업 개요 엄격히 금지)
    sys_instruction = """당신은 냉철하고 분석적인 실전 주식 트레이더입니다.
절대로 대상 기업이 '무엇을 하는 회사인지(사업 내용, 연혁 등)' 설명하지 마십시오.
제공된 차트 지표, 마스터 타점, 수급(프로그램) 데이터를 바탕으로 '현재 기술적 위치'와 '앞으로의 대응 전략(매수/관망/손절)'만을 2~3줄로 매우 짧고 날카롭게 작성하세요."""

    for index, row in df_scanner.iterrows():
        # "대기중"이라는 단어가 포함되어 있을 때만 AI 브리핑 생성
        if pd.notna(row.get('🤖 AI 종합 브리핑')) and "대기중" in str(row['🤖 AI 종합 브리핑']):
            stock_name = row['종목명']
            print(f" - [{stock_name}] 브리핑 작성 중...")
            
            prompt = f"""
            [{sys_instruction}]
            
            ■ 종목명: {stock_name}
            ■ 소속테마: {row.get('테마명', '알수없음')}
            ■ 현재가/등락률: {row.get('현재가', '')} ({row.get('등락률 ', '')})
            ■ 타점 위치: {row.get('마스터 타점', '')}
            ■ 당일 프로그램 수급: {row.get('프로그램(당일)', '')}
            """
            
            try:
                response = model.generate_content(prompt)
                briefing_text = response.text.strip()
                # 텍스트 교체
                df_scanner.at[index, '🤖 AI 종합 브리핑'] = briefing_text
                time.sleep(2) # API 토큰 제한 방지
            except Exception as e:
                print(f"[{stock_name}] 브리핑 생성 실패: {e}")
                df_scanner.at[index, '🤖 AI 종합 브리핑'] = "⚠️ 브리핑 생성 중 오류 발생"

    # ★ 중요: 작성된 내용을 덮어쓰기로 저장해야 "대기중"이 영구히 사라집니다.
    df_scanner.to_csv(DB_SCANNER_PATH, index=False, encoding='utf-8-sig')
    print("✅ DB_스캐너 브리핑 업데이트 및 파일 저장 완료.\n")


# ==========================================
# [2단계] 주가데이터_보조 150개 추출 (HYEOKS 점수 30점 이상 철저 검증)
# ==========================================
def extract_score(score_str):
    """ '40점 (돌파)' 등의 텍스트에서 숫자(40)만 추출하는 헬퍼 함수 """
    try:
        numbers = re.findall(r'\d+', str(score_str))
        if numbers:
            return int(numbers[0])
        return 0
    except:
        return 0

def pick_top_stocks_from_pool():
    DATA_AUX_PATH = "주식정리 - 주가데이터_보조.csv"
    print("▶ 주가데이터_보조에서 HYEOKS 퀀트 점수 기반 알파 종목 발굴 시작...")
    
    try:
        df_aux = pd.read_csv(DATA_AUX_PATH, encoding='utf-8-sig')
    except Exception as e:
        print(f"주가데이터_보조 로드 실패: {e}")
        return None

    # 1. HYEOKS점수 숫자형으로 변환
    df_aux['score_num'] = df_aux['HYEOKS점수'].apply(extract_score)
    
    # 2. 30점 이상인 종목만 1차 필터링
    df_high_score = df_aux[df_aux['score_num'] >= 30]
    
    # 만약 30점 이상 종목이 10개도 안 될 정도로 시장이 박살났다면, 차선책으로 전체에서 점수 높은 순으로 150개 가져오기
    if len(df_high_score) < 10:
        pool_150 = df_aux.sort_values(by='score_num', ascending=False).head(150)
    else:
        # 정상적인 시장이라면 30점 이상 종목 중에서 점수 내림차순으로 최대 150개 자르기
        pool_150 = df_high_score.sort_values(by='score_num', ascending=False).head(150)

    # 제미나이에게 던져줄 프롬프트용 텍스트 조립
    pool_str = ""
    for idx, row in pool_150.iterrows():
        pool_str += f"종목명:{row['종목명']} | HYEOKS점수:{row['score_num']}점 | 타점:{row.get('마스터타점','')} | 수급:{row.get('프로그램(당일)','')}\n"

    prompt = f"""
    당신은 대한민국 최고의 주식 트레이더이자 퀀트 분석가입니다.
    아래 제공된 데이터는 HYEOKS 퀀트 점수 30점 이상의 최상위 엘리트 150개 종목 리스트입니다.
    
    [핵심 임무]
    이 중에서 제미나이 2.5의 직관과 종합적인 판단(숨겨진 모멘텀, 테마 강도 등)을 활용해 최고의 단기 1종목, 스윙 1종목을 과감히 발굴해 내십시오.

    정확히 다음 2개의 종목을 골라주세요.
    1. 단기 슈팅 공략주: 오늘 수급이 몰리며 전고점 돌파를 목전에 둔 파괴력 있는 단기 종목 1개.
    2. 스윙 플랫폼 공략주: 바닥에서 에너지를 응축하고 턴어라운드를 시도하는 안정적인 스윙 종목 1개.

    [상위 150개 종목 리스트 (HYEOKS 점수순)]
    {pool_str}
    
    [출력 형식]
    반드시 아래 JSON 형식으로만 응답하세요. 다른 설명은 절대 추가하지 마세요.
    {{
        "short_term_pick": "종목명",
        "short_term_reason": "선정 사유 (수급, 차트 패턴 등 과감한 직관적 판단 근거)",
        "swing_pick": "종목명",
        "swing_reason": "선정 사유"
    }}
    """

    try:
        response = model.generate_content(prompt)
        cleaned_text = response.text.replace('```json', '').replace('```', '').strip()
        picks = json.loads(cleaned_text)
        
        print(f"🔥 HYEOKS 단기 픽: {picks['short_term_pick']}")
        print(f"🔥 HYEOKS 스윙 픽: {picks['swing_pick']}\n")
        return picks
        
    except Exception as e:
        print(f"종목 발굴 중 오류 발생: {e}")
        return None
