"""Install a live, read-only-of-ledger Google Sheets quality view.

All derived values are native formulas: new ledger rows recalculate without an
LLM call or another workflow. This is coverage, NOT a live-investment gate.
No calendar, no-signal, policy, price or 15:05 execution proof is invented.
"""
import argparse
import json

TITLE = "데이터_품질표"
VERSION = "quality-v2"
CHANNELS = [("차트TOP2", 5), ("수급TOP2", 5), ("랜덤2", 5),
            ("랜덤2_배지", 5), ("리포트TOP2_단기", 5),
            ("리포트TOP2_중기", 10), ("리포트TOP2_장기", 60)]
COLS = {5: ("T", "X"), 10: ("U", "Y"), 60: ("AB", "AE")}
SOURCE = "백테스트_로그"
DAILY_START = 30
DAYS = 120  # visible rolling calendar window, not claimed trading days


def ref(col):
    return f"'{SOURCE}'!${col}$2:${col}"


def validate_header(header):
    expected = {0: "trade_id", 1: "진입일", 2: "채널", 16: "진입가(T+1시가)",
                19: "종목T+5", 20: "종목T+10", 23: "지수T+5", 24: "지수T+10",
                25: "실제캡처거래일", 27: "종목T+60", 30: "지수T+60"}
    if any(len(header) <= i or header[i] != value for i, value in expected.items()):
        raise ValueError("백테스트_로그 header changed; refusing stale column mappings")


def mask(channel, day=None, horizon=None):
    parts = [f'({ref("C")}={channel})', f'ISERROR(SEARCH("제외",{ref("Z")}))']
    if day:
        parts.append(f'({ref("B")}={day})')
    if horizon:
        stock, index = COLS[horizon]
        parts += [f'ISNUMBER({ref("Q")})', f'IFERROR({ref("Q")}>0,FALSE)',
                  f'ISNUMBER({ref(stock)})', f'ISNUMBER({ref(index)})']
    return "*".join(parts)


def pair_count(channel, horizon, day=None):
    return f"SUMPRODUCT({mask(channel, day, horizon)})"


def pair_mean(channel, horizon, day):
    """그날 그 채널의 **지수 차감 후** 평균.

    v1 은 종목 수익률만 평균냈다. 그러면 코스피 종목을 많이 담은 채널과 코스닥
    종목을 많이 담은 채널의 차이에 **실력이 아닌 시장 구성 차이**가 섞인다.
    원장은 행마다 그 종목의 벤치로 지수를 채워 두므로(omakase.py 의 `벤치` 열),
    행 단위로 빼면 그 몫이 사라진다. 비용(0.35%)은 채널 간 차이에서 정확히
    상쇄되는 상수라 여기서는 빼지 않는다 — 그래서 '순알파'가 아니라 '초과'다.
    """
    stock, index = COLS[horizon]
    n = pair_count(channel, horizon, day)
    total = (f'SUMPRODUCT({mask(channel, day, horizon)}*'
             f'(IFERROR({ref(stock)}*1,0)-IFERROR({ref(index)}*1,0)))')
    return f'=IF({n}=0,"",{total}/{n})'


def build_rows():
    rows = [["채널", "기준", "원장 행", "제외 행", "집계 대상", "양수 진입가", "수익·지수 짝", "기록 비율", "기록 상태"]]
    for row, (ch, h) in enumerate(CHANNELS, 2):
        n = pair_count(f"$A{row}", h)
        rows.append([ch, f"T+{h}", f'=COUNTIF({ref("C")},A{row})',
                     f'=COUNTIFS({ref("C")},A{row},{ref("Z")},"*제외*")',
                     f'=C{row}-D{row}',
                     f'=SUMPRODUCT({mask(f"$A{row}")}*ISNUMBER({ref("Q")})*IFERROR({ref("Q")}>0,FALSE))',
                     f'={n}', f'=IF(E{row}=0,"",G{row}/E{row})',
                     f'=IF(C{row}=0,"기록 없음: 무신호/누락 구분 불가",IF(G{row}<E{row},"미성숙 또는 결손: 원인 미검증","기록 있음: 독립검증 전"))'])
    rows.append(["종베 15:05", "별도 체결 기준", "미연결", "", "", "", "", "", "T+1 시가 원장을 종베 실적으로 대체하지 않음"])
    rows += [[], ["오늘(수식 기준)", "=TODAY()", VERSION],
             ["주의", "기록 비율은 미성숙 행을 포함. 품질 합격률/수익성/실전 승인 아님."],
             ["미검증", "거래일 달력·수집 실패/무신호·후보모집단·정책 버전·입력 동결·가격/체결 독립검증은 이 원장만으로 확인 불가."],
             ["열 구조", '=IF(AND(\'백테스트_로그\'!B1="진입일",\'백테스트_로그\'!C1="채널",\'백테스트_로그\'!Q1="진입가(T+1시가)",\'백테스트_로그\'!T1="종목T+5",\'백테스트_로그\'!U1="종목T+10",\'백테스트_로그\'!X1="지수T+5",\'백테스트_로그\'!Y1="지수T+10",\'백테스트_로그\'!Z1="실제캡처거래일",\'백테스트_로그\'!AB1="종목T+60",\'백테스트_로그\'!AE1="지수T+60"),"일치(내용 정확성은 미검증)","오류: 열 변경, 아래 수치 사용 중지")'],
             ["기간", f"동일일 비교는 최근 {DAYS}일(달력일), 날짜별 동등 가중. 전체 기간 대시보드와 구분."],
             ["비교 한계", "동일일 ≠ 동일 후보군/정책. 미성숙·결손 제외된 조건부 결과이며 유의성 검정/계좌 수익 아님."],
             ["지수·비용", "v2 부터 **행마다 그 종목의 벤치(코스피/코스닥) 지수를 뺀다.** v1 은 종목 수익률만 평균내서 채널 간 시장 구성 차이가 실력 차이로 보였다. 비용(0.35%)은 채널 간 차이에서 상쇄되는 상수라 빼지 않는다 — 그래서 '순알파'가 아니라 '초과'다. 손절/트레일링/슬리피지 미반영."],
             [], ["동일 추천일 비교", "공통 날짜 수", "평균 차이(%p)", "판정"]]
    end = DAILY_START + DAYS - 1
    for label, col in [("단기 − 차트 T+5", "G"), ("단기 − 랜덤 T+5", "H"),
                       ("중기 − 차트 T+10", "N"), ("중기 − 랜덤 T+10", "O")]:
        r = len(rows) + 1
        rows.append([label, f'=COUNT({col}{DAILY_START}:{col}{end})',
                     f'=IF(B{r}=0,"",AVERAGE({col}{DAILY_START}:{col}{end})*100)',
                     "기술통계만: 우월성 미확정"])
    while len(rows) < DAILY_START - 2:
        rows.append([])
    rows[24] = ["원장 최근 추천일", f'=IF(COUNT({ref("B")})=0,"",MAX({ref("B")}))',
                "오늘과 다르다고 수집 실패는 아님: 거래일/무신호 기록 별도 필요"]
    rows[25] = ["trade_id 중복 관련 행", f'=SUMPRODUCT(({ref("A")}<>"")*(COUNTIF({ref("A")},{ref("A")})>1))',
                "0 초과 시 원장 확인. 이 표는 중복 행을 임의 삭제하지 않음."]
    rows.append(["날짜(달력일)", "단기 추천 행", "단기 짝 표본", "단기 초과T+5", "차트 초과T+5", "랜덤 초과T+5", "단기−차트", "단기−랜덤",
                 "중기 추천 행", "중기 짝 표본", "중기 초과T+10", "차트 초과T+10", "랜덤 초과T+10", "중기−차트", "중기−랜덤"])
    for offset in range(DAYS):
        r = DAILY_START + offset
        day = f"$A{r}"
        values = [f'=TODAY()-{offset}']
        for ch, h, ncol, stockcol, chartcol, randomcol in [
                ("리포트TOP2_단기", 5, "C", "D", "E", "F"),
                ("리포트TOP2_중기", 10, "J", "K", "L", "M")]:
            channel = f'"{ch}"'
            values += [f'=COUNTIFS({ref("C")},{channel},{ref("B")},{day})',
                       f'={pair_count(channel,h,day)}',
                       pair_mean(channel, h, day), pair_mean('"차트TOP2"', h, day), pair_mean('"랜덤2"', h, day),
                       f'=IF(AND(ISNUMBER({stockcol}{r}),ISNUMBER({chartcol}{r})),{stockcol}{r}-{chartcol}{r},"")',
                       f'=IF(AND(ISNUMBER({stockcol}{r}),ISNUMBER({randomcol}{r})),{stockcol}{r}-{randomcol}{r},"")']
        rows.append(values)
    return rows


def sheet_requests(sheet_id, add=False):
    rows = build_rows()
    requests = []
    if add:
        requests.append({"addSheet": {"properties": {"sheetId": sheet_id, "title": TITLE,
                         "gridProperties": {"rowCount": len(rows), "columnCount": 15}}}})
    def cell(v):
        return {"userEnteredValue": {"formulaValue" if isinstance(v, str) and v.startswith("=") else "stringValue": v}}
    requests.append({"updateCells": {"start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 0},
                     "rows": [{"values": [cell(v) for v in row]} for row in rows], "fields": "userEnteredValue"}})
    requests += [{"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1}}, "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
                 {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 15}, "properties": {"pixelSize": 112}, "fields": "pixelSize"}},
                 {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 195}, "fields": "pixelSize"}},
                 {"repeatCell": {"range": {"sheetId": sheet_id}, "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 10}, "verticalAlignment": "MIDDLE"}}, "fields": "userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment"}}]
    for start, end in [(0, 1), (18, 19), (DAILY_START-2, DAILY_START-1)]:
        requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": start, "endRowIndex": end}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": .93, "green": .93, "blue": .93}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.bold,userEnteredFormat.wrapStrategy"}})
    for sr, er, sc, ec, typ, pattern in [(1, 8, 7, 8, "PERCENT", "0.0%"),
            (10, 11, 1, 2, "DATE", "yyyy-mm-dd"), (24, 25, 1, 2, "DATE", "yyyy-mm-dd"), (DAILY_START-1, len(rows), 0, 1, "DATE", "yyyy-mm-dd"),
            (DAILY_START-1, len(rows), 3, 8, "PERCENT", "0.00%;-0.00%"),
            (DAILY_START-1, len(rows), 10, 15, "PERCENT", "0.00%;-0.00%"),
            (19, 23, 2, 3, "NUMBER", "0.00;-0.00")]:
        requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": sr, "endRowIndex": er, "startColumnIndex": sc, "endColumnIndex": ec}, "cell": {"userEnteredFormat": {"numberFormat": {"type": typ, "pattern": pattern}}}, "fields": "userEnteredFormat.numberFormat"}})
    for r in range(11, 17):
        requests.append({"mergeCells": {"range": {"sheetId": sheet_id, "startRowIndex": r, "endRowIndex": r+1, "startColumnIndex": 1, "endColumnIndex": 15}, "mergeType": "MERGE_ALL"}})
    requests += [
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 9, "startColumnIndex": 8, "endColumnIndex": 9}, "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.wrapStrategy"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 9}, "properties": {"pixelSize": 32}, "fields": "pixelSize"}},
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 11, "endRowIndex": 17}, "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.wrapStrategy"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 11, "endIndex": 17}, "properties": {"pixelSize": 36}, "fields": "pixelSize"}},
    ]
    for r in range(1, 9):
        requests.append({"mergeCells": {"range": {"sheetId": sheet_id, "startRowIndex": r, "endRowIndex": r+1, "startColumnIndex": 8, "endColumnIndex": 15}, "mergeType": "MERGE_ALL"}})
    if add:
        white = {"rgbColor": {"red": 1, "green": 1, "blue": 1}}
        requests.append({"addTable": {"table": {"name": "quality_daily_v1",
                         "range": {"sheetId": sheet_id, "startRowIndex": 28, "endRowIndex": len(rows), "startColumnIndex": 0, "endColumnIndex": 15},
                         "rowsProperties": {"headerColorStyle": {"rgbColor": {"red": .93, "green": .93, "blue": .93}},
                                            "firstBandColorStyle": white, "secondBandColorStyle": white}}}})
    # Native tables may default to white header text; keep contrast on gray.
    requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 28, "endRowIndex": 29},
                     "cell": {"userEnteredFormat": {"textFormat": {"foregroundColorStyle": {"rgbColor": {"red": 0, "green": 0, "blue": 0}}}}},
                     "fields": "userEnteredFormat.textFormat.foregroundColorStyle"}})
    return requests


def check_document(doc):
    """Read-only live monitoring; exceptions intentionally fail the workflow."""
    validate_header(doc.worksheet(SOURCE).row_values(1))
    rows = doc.worksheet(TITLE).get_all_values()
    if len(rows) != DAILY_START + DAYS - 1 or rows[10][2] != VERSION:
        raise ValueError("quality view absent, incomplete or version mismatch")
    errors = ("#REF!", "#ERROR!", "#VALUE!", "#DIV/0!", "#N/A", "#NAME?", "#NUM!")
    if any(str(v).startswith(errors) for row in rows for v in row):
        raise ValueError("quality view contains calculation errors")
    if not rows[13][1].startswith("일치"):
        raise ValueError("quality view source header mismatch")
    formulas = doc.worksheet(TITLE).get("A1:O149", value_render_option="FORMULA")
    expected = build_rows()
    # Numeric snapshots can look healthy while silently breaking automation.
    for i, row in enumerate(expected):
        for j, value in enumerate(row):
            if isinstance(value, str) and value.startswith("="):
                if len(formulas) <= i or len(formulas[i]) <= j or formulas[i][j] != value:
                    raise ValueError(f"quality formula changed at row {i+1}, column {j+1}")
    print("Quality view: schema and formulas intact. Coverage only; investment approval NOT evaluated.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, help="print installation requests; no credentials or source data")
    parser.add_argument("--existing", action="store_true",
                        help="이미 있는 탭을 덮어쓴다 — addSheet 를 빼고 셀만 갱신한다")
    parser.add_argument("--out", help="표준출력 대신 이 파일로 쓴다")
    parser.add_argument("--check", action="store_true", help="read-only live check using existing secret.json")
    parser.add_argument("--spreadsheet-id")
    args = parser.parse_args()
    if args.requests is not None:
        # --existing 이면 addSheet 를 빼고 addTable 도 뺀다. 이미 있는 탭에 다시
        # 만들려 들면 Sheets API 가 통째로 거절한다.
        body = json.dumps({"requests": sheet_requests(args.requests,
                                                      add=not args.existing)},
                          ensure_ascii=False, indent=1)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(body + "\n")
            print(f"{args.out}: {len(body)} bytes · sheetId={args.requests} · "
                  f"{'덮어쓰기' if args.existing else '신규 설치'} · {VERSION}")
        else:
            print(body)
    if args.check:
        if not args.spreadsheet_id:
            parser.error("--check requires --spreadsheet-id")
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            "secret.json", ["https://www.googleapis.com/auth/spreadsheets.readonly"])
        check_document(gspread.authorize(credentials).open_by_key(args.spreadsheet_id))


if __name__ == "__main__":
    main()
