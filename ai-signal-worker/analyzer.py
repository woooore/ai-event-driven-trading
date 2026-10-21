import os
import sys
import time
import argparse
import hashlib
import json
import re
from typing import List, Optional, Dict
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field
from confluent_kafka import Producer
import yfinance as yf
import feedparser

# Windows 콘솔 utf-8 인코딩 설정
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# 1. 환경변수 및 OpenAI 클라이언트 초기화
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("[경고] OPENAI_API_KEY가 .env 파일에 설정되지 않았습니다.")
client = OpenAI(api_key=api_key)

# 2. 미국 시총 상위 50대 우량주 유니버스 풀 (S&P 500 / NASDAQ Top 50)
TOP_50_US_STOCKS: Dict[str, Dict[str, str]] = {
    # 빅테크 & 반도체
    "NVDA": {"name": "NVIDIA", "keywords": ["nvidia", "nvda", "jensen huang"], "exchange": "NASD"},
    "AAPL": {"name": "Apple", "keywords": ["apple", "aapl", "iphone", "tim cook"], "exchange": "NASD"},
    "MSFT": {"name": "Microsoft", "keywords": ["microsoft", "msft", "satya nadella", "azure"], "exchange": "NASD"},
    "AMZN": {"name": "Amazon", "keywords": ["amazon", "amzn", "aws"], "exchange": "NASD"},
    "GOOGL": {"name": "Alphabet (Google)", "keywords": ["google", "googl", "goog", "alphabet", "sundar pichai"], "exchange": "NASD"},
    "META": {"name": "Meta Platforms", "keywords": ["meta", "facebook", "zuckerberg", "instagram"], "exchange": "NASD"},
    "TSLA": {"name": "Tesla", "keywords": ["tesla", "tsla", "elon musk", "cybertruck", "robotaxi"], "exchange": "NASD"},
    "AVGO": {"name": "Broadcom", "keywords": ["broadcom", "avgo"], "exchange": "NASD"},
    "AMD": {"name": "AMD", "keywords": ["amd", "lisa su", "radeon"], "exchange": "NASD"},
    "QCOM": {"name": "Qualcomm", "keywords": ["qualcomm", "qcom", "snapdragon"], "exchange": "NASD"},
    "NFLX": {"name": "Netflix", "keywords": ["netflix", "nflx"], "exchange": "NASD"},
    "ORCL": {"name": "Oracle", "keywords": ["oracle", "orcl", "larry ellison"], "exchange": "NYSE"},
    "CRM": {"name": "Salesforce", "keywords": ["salesforce", "crm"], "exchange": "NYSE"},
    "ADBE": {"name": "Adobe", "keywords": ["adobe", "adbe"], "exchange": "NASD"},
    "CSCO": {"name": "Cisco", "keywords": ["cisco", "csco"], "exchange": "NASD"},
    "INTU": {"name": "Intuit", "keywords": ["intuit", "intu"], "exchange": "NASD"},
    "IBM": {"name": "IBM", "keywords": ["ibm"], "exchange": "NYSE"},
    "AMAT": {"name": "Applied Materials", "keywords": ["applied materials", "amat"], "exchange": "NASD"},
    "TXN": {"name": "Texas Instruments", "keywords": ["texas instruments", "txn"], "exchange": "NASD"},
    "NOW": {"name": "ServiceNow", "keywords": ["servicenow", "now"], "exchange": "NYSE"},

    # 금융 / 투자
    "BRK.B": {"name": "Berkshire Hathaway", "keywords": ["berkshire", "warren buffett", "brk"], "exchange": "NYSE"},
    "JPM": {"name": "JPMorgan Chase", "keywords": ["jpmorgan", "jpm", "jamie dimon"], "exchange": "NYSE"},
    "V": {"name": "Visa", "keywords": ["visa"], "exchange": "NYSE"},
    "MA": {"name": "Mastercard", "keywords": ["mastercard"], "exchange": "NYSE"},
    "BAC": {"name": "Bank of America", "keywords": ["bank of america", "bofa"], "exchange": "NYSE"},
    "WFC": {"name": "Wells Fargo", "keywords": ["wells fargo", "wfc"], "exchange": "NYSE"},

    # 헬스케어 / 바이오
    "LLY": {"name": "Eli Lilly", "keywords": ["eli lilly", "lly", "mounjaro", "zepbound"], "exchange": "NYSE"},
    "UNH": {"name": "UnitedHealth", "keywords": ["unitedhealth", "unh"], "exchange": "NYSE"},
    "JNJ": {"name": "Johnson & Johnson", "keywords": ["johnson & johnson", "jnj"], "exchange": "NYSE"},
    "ABBV": {"name": "AbbVie", "keywords": ["abbvie", "abbv"], "exchange": "NYSE"},
    "MRK": {"name": "Merck", "keywords": ["merck", "mrk", "keytruda"], "exchange": "NYSE"},
    "TMO": {"name": "Thermo Fisher", "keywords": ["thermo fisher", "tmo"], "exchange": "NYSE"},
    "DHR": {"name": "Danaher", "keywords": ["danaher", "dhr"], "exchange": "NYSE"},
    "ISRG": {"name": "Intuitive Surgical", "keywords": ["intuitive surgical", "isrg"], "exchange": "NASD"},

    # 필수소비재 / 유통 / 서비스
    "WMT": {"name": "Walmart", "keywords": ["walmart", "wmt"], "exchange": "NYSE"},
    "COST": {"name": "Costco", "keywords": ["costco", "cost"], "exchange": "NASD"},
    "HD": {"name": "Home Depot", "keywords": ["home depot", "hd"], "exchange": "NYSE"},
    "PG": {"name": "Procter & Gamble", "keywords": ["procter & gamble", "p&g", "pg"], "exchange": "NYSE"},
    "KO": {"name": "Coca-Cola", "keywords": ["coca-cola", "coke", "ko"], "exchange": "NYSE"},
    "PEP": {"name": "PepsiCo", "keywords": ["pepsico", "pepsi", "pep"], "exchange": "NASD"},
    "MCD": {"name": "McDonald's", "keywords": ["mcdonald's", "mcdonalds", "mcd"], "exchange": "NYSE"},
    "DIS": {"name": "Walt Disney", "keywords": ["disney", "dis", "bob iger"], "exchange": "NYSE"},
    "NKE": {"name": "Nike", "keywords": ["nike", "nke"], "exchange": "NYSE"},
    "PM": {"name": "Philip Morris", "keywords": ["philip morris", "pm"], "exchange": "NYSE"},
    "BKNG": {"name": "Booking Holdings", "keywords": ["booking holdings", "bkng"], "exchange": "NASD"},

    # 에너지 / 산업재
    "XOM": {"name": "Exxon Mobil", "keywords": ["exxon", "xom", "mobil"], "exchange": "NYSE"},
    "CVX": {"name": "Chevron", "keywords": ["chevron", "cvx"], "exchange": "NYSE"},
    "LIN": {"name": "Linde", "keywords": ["linde", "lin"], "exchange": "NYSE"},
    "CAT": {"name": "Caterpillar", "keywords": ["caterpillar", "cat"], "exchange": "NYSE"},
    "GE": {"name": "GE Aerospace", "keywords": ["ge aerospace", "general electric"], "exchange": "NYSE"}
}

# 3. RAG 기반 매매 일지 (Trading Journal) 시스템
JOURNAL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trading_journal.json")

def load_trading_journal() -> List[dict]:
    if not os.path.exists(JOURNAL_FILE):
        return []
    try:
        with open(JOURNAL_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[매매 일지 로드 실패]: {e}")
        return []

def search_trading_journal(ticker: str, headline: str, summary: str, top_k: int = 2) -> List[dict]:
    """과거 매매 일지에서 유사 헤드라인 또는 해당 종목의 과거 손실(실패) 기록을 검색(RAG)"""
    journal = load_trading_journal()
    if not journal:
        return []

    tokens = set(re.findall(r'\b[a-zA-Z]{3,}\b', (headline + " " + summary).lower()))
    
    scored_items = []
    for item in journal:
        score = 0.0
        # 1. 동일 종목 가중치
        if item.get("ticker", "").upper() == ticker.upper():
            score += 3.0
        
        # 2. 키워드 유사도 가중치
        past_text = (item.get("headline", "") + " " + item.get("summary", "")).lower()
        past_tokens = set(re.findall(r'\b[a-zA-Z]{3,}\b', past_text))
        overlap = tokens.intersection(past_tokens)
        score += len(overlap) * 1.5

        # 3. 손실 기록 우선 가중치 (손실이 컸던 사례를 우선 반면교사로 삼음)
        pnl_pct = item.get("pnl_pct", 0.0)
        if pnl_pct < 0:
            score += abs(pnl_pct) * 0.5

        if score >= 2.0:
            scored_items.append((score, item))

    scored_items.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored_items[:top_k]]

def record_trade_journal(ticker: str, headline: str, summary: str, ai_reason: str, action: str, pnl_usd: float, pnl_pct: float, lesson: str):
    """체결 후 청산 결과 또는 새로운 매매 일지를 DB(JSON)에 추가 기록"""
    journal = load_trading_journal()
    entry = {
        "id": f"log-{int(time.time())}",
        "ticker": ticker.upper(),
        "headline": headline,
        "summary": summary,
        "ai_reason": ai_reason,
        "action": action,
        "pnl_usd": round(pnl_usd, 2),
        "pnl_pct": round(pnl_pct, 2),
        "lesson_learned": lesson
    }
    journal.append(entry)
    try:
        with open(JOURNAL_FILE, "w", encoding="utf-8") as f:
            json.dump(journal, f, ensure_ascii=False, indent=2)
        print(f">> [매매 일지 기록 완료] {ticker} ({pnl_pct:+}%) 반면교사 DB 저장")
    except Exception as e:
        print(f"[매매 일지 저장 오류]: {e}")

# 4. 기술적 보조 지표 계산기 (RSI 14 & 20일 이동평균선 이격도)
def calculate_technical_indicators(closes: list, period: int = 14) -> tuple[Optional[float], Optional[float]]:
    if len(closes) < period + 1:
        return None, None
    try:
        diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0.0 for d in diffs]
        losses = [-d if d < 0 else 0.0 for d in diffs]

        # Wilder's Smoothing RSI
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(diffs)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

        current_price = float(closes[-1])
        ma20_window = closes[-min(20, len(closes)):]
        ma20 = float(sum(ma20_window) / len(ma20_window))
        disparity = round((current_price / ma20) * 100.0, 2) if ma20 > 0 else 100.0

        return round(rsi, 2), disparity
    except Exception as e:
        print(f"[보조지표 계산 오류]: {e}")
        return None, None

# 5. AI 트레이딩 판단 JSON 스키마 (Pydantic Structured Output)
class StockSignal(BaseModel):
    ticker: str = Field(description="미국 주식 티커 심볼 (예: NVDA, AAPL, TSLA, MSFT)")
    stock_name: str = Field(description="기업명 (예: NVIDIA Corporation)")
    exchange: str = Field(description="거래소 코드 (NASD, NYSE, AMEX)")
    action: str = Field(description="트레이딩 판단 액션: BUY, SELL, HOLD 중 하나")
    confidence: float = Field(description="AI 판단의 확신도 점수 (0.0 ~ 1.0). 단순 노이즈는 0.5 미만")
    order_price: float = Field(description="현재가 기반 진입 권장 단가 (USD)")
    stop_loss: float = Field(description="리스크 관리를 위한 손절 기준가 (USD)")
    take_profit: float = Field(description="목표 익절 기준가 (USD)")
    sentiment: str = Field(description="POSITIVE, NEGATIVE, NEUTRAL 중 하나")
    reason: str = Field(description="AI의 핵심 투자 논거 및 매매 근거 요약 (1~2문장)")
    rsi: Optional[float] = Field(default=None, description="14일 상대강도지수 (RSI)")
    ma_disparity: Optional[float] = Field(default=None, description="20일 이동평균선 이격도 (%)")

# 6. Kafka Producer 설정
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092'),
    'client.id': 'ai-signal-worker'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f">> [Kafka 전송 실패]: {err}")
    else:
        print(f">> [Kafka 전송 완료] Topic: {msg.topic()} | Partition: [{msg.partition()}]")

# 7. 종목별 시세, 기술적 지표 및 뉴스 수집기
def get_market_and_news(ticker: str, custom_news_list: Optional[List[dict]] = None):
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="1mo")
        
        current_price = None
        prev_close = None
        rsi = None
        disparity = None

        if not hist.empty and len(hist) >= 2:
            closes = hist['Close'].tolist()
            current_price = float(closes[-1])
            prev_close = float(closes[-2])
            rsi, disparity = calculate_technical_indicators(closes, period=14)
        else:
            fast_info = getattr(t, 'fast_info', None)
            if fast_info:
                current_price = getattr(fast_info, 'last_price', None)
                prev_close = getattr(fast_info, 'previous_close', None)

        pct_change = 0.0
        if current_price and prev_close and prev_close > 0:
            pct_change = round(((current_price - prev_close) / prev_close) * 100, 2)

        if custom_news_list:
            news_items = custom_news_list
        else:
            rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
            feed = feedparser.parse(rss_url)
            news_items = []
            for entry in feed.entries[:3]:
                news_items.append({
                    'title': entry.get('title', ''),
                    'summary': entry.get('summary', ''),
                    'link': entry.get('link', ''),
                    'published': entry.get('published', '')
                })

        return {
            'ticker': ticker,
            'current_price': round(current_price, 2) if current_price else 100.0,
            'pct_change': pct_change,
            'rsi': rsi,
            'ma_disparity': disparity,
            'news': news_items
        }
    except Exception as e:
        print(f"[데이터 수집 오류] {ticker}: {e}")
        return None

# 8. AI 퀀트 트레이딩 판단 모델 (RAG 매매 일지 + 프롬프트 룰 + 기술적 필터링)
def evaluate_market_and_trade(market_data: dict) -> Optional[StockSignal]:
    ticker = market_data['ticker']
    current_price = market_data['current_price']
    pct_change = market_data['pct_change']
    rsi = market_data.get('rsi')
    ma_disparity = market_data.get('ma_disparity')
    news_items = market_data['news']

    if not news_items:
        return None

    news_text = "\n".join([
        f"- [제목]: {n['title']}\n  [요약]: {n['summary']}"
        for n in news_items
    ])

    info = TOP_50_US_STOCKS.get(ticker, {"name": ticker, "exchange": "NASD"})
    company_name = info["name"]
    exchange = info.get("exchange", "NASD")

    # RAG: 과거 매매 일지에서 해당 종목 또는 유사 기사의 실패(손실) 기록 검색
    headline_first = news_items[0]['title'] if news_items else ""
    summary_first = news_items[0]['summary'] if news_items else ""
    matched_journals = search_trading_journal(ticker, headline_first, summary_first, top_k=2)

    rag_section = ""
    if matched_journals:
        rag_section = "\n[과거 매매 일지 RAG 검색: 유사 기사 및 손실(실패) 반면교사 기록]\n"
        for j in matched_journals:
            rag_section += (
                f"- 과거 종목: {j.get('ticker')} | 과거 헤드라인: \"{j.get('headline')}\"\n"
                f"  당시 AI 매수 사유: {j.get('ai_reason')}\n"
                f"  실제 매매 결과: 손익 {j.get('pnl_pct', 0.0):+}% ({j.get('pnl_usd', 0.0):+} USD 손실)\n"
                f"  ★ 핵심 교훈: {j.get('lesson_learned')}\n"
            )
        rag_section += "=> [엄격 지침]: 과거 이와 유사한 기사에서 실제 손실이 발생했으니 동일한 실수를 반복하지 말고 지극히 보수적으로 평가(HOLD)할 것!\n"

    # 기술적 지표 텍스트
    tech_str = f"RSI(14): {rsi:.1f} | 20일 이격도: {ma_disparity:.1f}%" if (rsi is not None and ma_disparity is not None) else "데이터 없음"

    system_prompt = (
        "너는 미국 월스트리트 헤지펀드의 시니어 이벤트 드리븐(Event-Driven) 퀀트 트레이더다.\n"
        "주어진 미국 주식의 현재가, 당일 변동률, 기술적 보조 지표(RSI 14, 20일 이격도), 과거 실패 매매 일지(RAG), 그리고 최신 뉴스를 종합 분석하여 트레이딩 결정을 내려라.\n\n"
        "【필수 매매 규칙 & 손실 방어 원칙 (Rule Updates)】:\n"
        "1. [선반영(Priced-in) 및 '뉴스에 팔아라(Sell the News)' 철저 방어]:\n"
        "   - 시장 세력은 호재성 기사를 띄워놓고 개인 투자자에게 물량을 떠넘기고 차익 실현한다.\n"
        "   - 헤드라인에 '기대감(Expectation, Anticipate, Hope)', '전망(Outlook, May, Could)', '애널리스트 목표가 상향(Target Raised)' 등 정량적 숫자가 없는 단순 기대 심리 기사는 무조건 action='HOLD', confidence <= 0.50으로 처리하라.\n"
        "2. [실적 발표 당일 단순 기대감 / 찌라시 매수 절대 금지]:\n"
        "   - 실적 발표 직전/당일의 '어닝 서프라이즈 기대감' 기사는 발표 직후 차익 실현 폭탄의 주원인이다. 확정 발표된 실적 수치(Beat)가 없는 기대감 기사는 절대 BUY를 내리지 마라.\n"
        "3. [기술적 과매수(과열) 필터링 준수]:\n"
        "   - RSI(14)가 70.0 이상이거나 20일 이격도가 110.0% 이상이면 이미 단기 천장(과매수)이다. 아무리 뉴스가 좋아도 추격 매수하지 말고 action='HOLD'로 결정하라.\n"
        "4. [구체적 수치 없는 추상적 찬사 및 의문형 기사 배제]:\n"
        "   - 물음표(?)가 붙은 추측성 헤드라인이나 구체적 수주액($)이 없는 단순 PR 기사는 노이즈로 간주하고 HOLD하라.\n"
        "5. [확실한 모멘텀에만 BUY]:\n"
        "   - 컨센서스를 대폭 상회한 공식 실적 서프라이즈, 구체적 금액이 명시된 대규모 신규 계약, FDA 최종 승인 등 즉각적 주가 폭등을 견인할 확실한 팩트가 있을 때만 action='BUY', confidence >= 0.70으로 결정하라.\n"
        "6. [악재 감지 시 즉시 긴급 탈출 SELL]:\n"
        "   - 대규모 소송 패소, 거액 배상금/과징금, 실적 쇼크, 규제 제재 시 즉시 action='SELL', confidence >= 0.75로 전량 탈출하라.\n"
        "7. BUY 결정 시:\n"
        f"   - order_price: 현재 시장 가격 (${current_price}) 기준\n"
        "   - take_profit: 약 +3.0% 수준의 빠른 단기 목표 익절가\n"
        "   - stop_loss: 약 -6.5% 수준의 안전 손절 기준가\n"
        f"   - exchange: '{exchange}'"
    )

    user_prompt = f"""
[분석 대상 종목]: {company_name} ({ticker}) [거래소: {exchange}]
[현재 시장 가격]: ${current_price} (당일 변동률: {pct_change:+}%)
[기술적 보조 지표]: {tech_str}
{rag_section}
[최신 뉴스 피드]:
{news_text}

위 데이터를 종합 평가하여 지정된 JSON 스키마로 트레이딩 결정을 반환하라.
"""

    try:
        response = client.beta.chat.completions.parse(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format=StockSignal,
        )
        signal: StockSignal = response.choices[0].message.parsed
        signal.rsi = rsi
        signal.ma_disparity = ma_disparity

        # 퀀트 기술적 지표 1차 하드 안전장치 (AI 환각이나 과열 오판 방어)
        if signal.action == "BUY":
            if rsi is not None and rsi >= 70.0:
                print(f"   🛡️ [퀀트 기술적 필터 차단] {ticker} RSI({rsi:.1f}) >= 70 과매수 과열! 세력 매물받이 방어로 HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.reason = f"[기술적 과매수 차단] RSI({rsi:.1f})가 70 이상 과열 상태로 단기 고점 세력 차익실현 덤핑 위험. {signal.reason}"
            elif ma_disparity is not None and ma_disparity >= 110.0:
                print(f"   🛡️ [퀀트 기술적 필터 차단] {ticker} 20일 이격도({ma_disparity:.1f}%) >= 110% 급등 과열! HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.reason = f"[이격도 과열 차단] 20일선 대비 {ma_disparity:.1f}% 급등 과열로 차익 실현 매물 위험. {signal.reason}"

        return signal
    except Exception as e:
        print(f"[AI 분석 실패] {ticker}: {e}")
        return None

# 9. 중복 기사 캐시
processed_news_hashes = set()

def process_single_ticker(ticker: str, custom_news: Optional[List[dict]] = None):
    market_data = get_market_and_news(ticker, custom_news)
    if not market_data or not market_data['news']:
        return

    news_concat = "".join([n['title'] for n in market_data['news']])
    content_hash = hashlib.md5(f"{ticker}:{news_concat}".encode('utf-8')).hexdigest()
    if content_hash in processed_news_hashes:
        return

    tech_info = f"RSI: {market_data['rsi']:.1f}" if market_data.get('rsi') else "RSI: N/A"
    print(f"\n>> [속보 포착] {ticker} ({TOP_50_US_STOCKS.get(ticker, {}).get('name', ticker)}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%) | {tech_info}")
    print(f"   뉴스 헤드라인: {market_data['news'][0]['title'][:70]}...")

    signal = evaluate_market_and_trade(market_data)
    if not signal:
        return

    processed_news_hashes.add(content_hash)

    print(f"   [AI 판단] 액션: {signal.action} | 확신도: {signal.confidence:.2f} | 감성: {signal.sentiment}")
    print(f"   [리스크] 단가: ${signal.order_price:.2f} | 익절가: ${signal.take_profit:.2f} | 손절가: ${signal.stop_loss:.2f}")
    print(f"   [투자 근거]: {signal.reason}")

    # BUY or SELL & 확신도 >= 0.70 일 때만 Kafka 전송
    if signal.action in ["BUY", "SELL"] and signal.confidence >= 0.70:
        payload = signal.model_dump_json()
        producer.produce('stock-signals', key=signal.ticker, value=payload, callback=delivery_report)
        producer.flush()
        print(f"   🚀 [시그널 발행 완료] {signal.action} {signal.ticker} -> Core Engine(1,000만원 포트폴리오) 전송 완료!")
    else:
        print(f"   ⏸️ [관망 유지] 조건 미달로 주문 미발행 (시장 관망)")

# 10. 실시간 전체 시장 종합 속보 스캐너 (시총 50위 종목 순환 및 속보 감지)
def scan_market_wide_news(max_tickers_per_batch: int = 15):
    """미국 시총 상위 50대 기업의 실시간 헤드라인을 순환 스캔하여 속보 발생 종목을 AI로 즉시 분석"""
    print(f">> [미국 시총 50위 유니버스 실시간 속보 스캔 중...]")
    all_tickers = list(TOP_50_US_STOCKS.keys())
    
    target_batch = all_tickers[:max_tickers_per_batch]
    
    analyzed_count = 0
    for ticker in target_batch:
        try:
            market_data = get_market_and_news(ticker)
            if not market_data or not market_data['news']:
                continue
            
            news_concat = "".join([n['title'] for n in market_data['news']])
            content_hash = hashlib.md5(f"{ticker}:{news_concat}".encode('utf-8')).hexdigest()
            if content_hash in processed_news_hashes:
                continue

            analyzed_count += 1
            tech_info = f"RSI: {market_data['rsi']:.1f}" if market_data.get('rsi') else "RSI: N/A"
            print(f"\n>> 🔍 [새로운 속보 감지] {ticker} ({TOP_50_US_STOCKS.get(ticker, {}).get('name', ticker)}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%) | {tech_info}")
            print(f"   헤드라인: {market_data['news'][0]['title'][:80]}")

            signal = evaluate_market_and_trade(market_data)
            if not signal:
                continue

            processed_news_hashes.add(content_hash)

            print(f"   [AI 판단] {signal.action} | 확신도: {signal.confidence:.2f} | 감성: {signal.sentiment}")
            print(f"   [리스크] 단가: ${signal.order_price:.2f} | 익절가: ${signal.take_profit:.2f} | 손절가: ${signal.stop_loss:.2f}")
            print(f"   [투자 근거]: {signal.reason}")

            if signal.action in ["BUY", "SELL"] and signal.confidence >= 0.70:
                payload = signal.model_dump_json()
                producer.produce('stock-signals', key=signal.ticker, value=payload, callback=delivery_report)
                producer.flush()
                print(f"   🚀 [시그널 발행 성공] {signal.action} {signal.ticker} -> Core Engine 전송 완료!")
            else:
                print(f"   ⏸️ [관망 유지] 조건 미달로 주문 미발행 (시장 관망)")

            time.sleep(1) # API 보호
        except Exception as e:
            print(f"[{ticker} 스캔 오류] {e}")

    if analyzed_count == 0:
        print(">> [스캔 결과] 기존 분석 완료된 뉴스 외 신규 속보 없음 (다음 주기 대기)")

# 11. 인텔리제이 연동 테스트용 가상 시그널 발송
def fire_sample_buy_signal(target_ticker: str = "NVDA"):
    info = TOP_50_US_STOCKS.get(target_ticker, {"name": target_ticker, "exchange": "NASD"})
    print(f"\n>> [테스트 모드] 실시간 {target_ticker} 현재가 조회 후 연동 검증용 BUY 시그널 생성 중...")
    market_data = get_market_and_news(target_ticker)
    live_price = market_data['current_price'] if market_data else 150.00
    live_rsi = market_data.get('rsi', 52.0) if market_data else 52.0
    live_disp = market_data.get('ma_disparity', 101.5) if market_data else 101.5

    sample_signal = StockSignal(
        ticker=target_ticker,
        stock_name=info["name"],
        exchange=info.get("exchange", "NASD"),
        action="BUY",
        confidence=0.92,
        order_price=round(live_price, 2),
        stop_loss=round(live_price * 0.935, 2),  # -6.5% 손절선
        take_profit=round(live_price * 1.03, 2), # +3.0% 단기 익절선
        sentiment="POSITIVE",
        reason=f"{info['name']} announces breakthrough enterprise growth and raised fiscal guidance beating Street estimates.",
        rsi=live_rsi,
        ma_disparity=live_disp
    )
    payload = sample_signal.model_dump_json()
    producer.produce('stock-signals', key=sample_signal.ticker, value=payload, callback=delivery_report)
    producer.flush()
    print(f">> [테스트 완료] 실시간 현재가(${live_price}) & RSI({live_rsi:.1f}) 기반 {target_ticker} 매수 시그널 발행 완료!")
    print(f"   내용: {payload}")

def fire_sample_sell_signal(target_ticker: str = "AAPL"):
    info = TOP_50_US_STOCKS.get(target_ticker, {"name": target_ticker, "exchange": "NASD"})
    print(f"\n>> [테스트 모드] 실시간 {target_ticker} 긴급 탈출 SELL 시그널 생성 중...")
    market_data = get_market_and_news(target_ticker)
    live_price = market_data['current_price'] if market_data else 220.00
    live_rsi = market_data.get('rsi', 65.0) if market_data else 65.0
    live_disp = market_data.get('ma_disparity', 105.0) if market_data else 105.0

    sample_signal = StockSignal(
        ticker=target_ticker,
        stock_name=info["name"],
        exchange=info.get("exchange", "NASD"),
        action="SELL",
        confidence=0.88,
        order_price=round(live_price, 2),
        stop_loss=0.0,
        take_profit=0.0,
        sentiment="NEGATIVE",
        reason=f"{info['name']} hit with multi-billion dollar patent infringement liability verdict causing immediate institutional exit.",
        rsi=live_rsi,
        ma_disparity=live_disp
    )
    payload = sample_signal.model_dump_json()
    producer.produce('stock-signals', key=sample_signal.ticker, value=payload, callback=delivery_report)
    producer.flush()
    print(f">> [테스트 완료] {target_ticker} 악재 감지 긴급 매도(SELL) 시그널 발행 완료!")
    print(f"   내용: {payload}")

# 10. 메인 실행 함수
def main():
    parser = argparse.ArgumentParser(description="미국 시총 50위 AI 이벤트 드리븐 트레이딩 워커")
    parser.add_argument("--loop", action="store_true", help="24시간 실시간 무한 모니터링 실행")
    parser.add_argument("--interval", type=int, default=60, help="모니터링 주기 (초)")
    parser.add_argument("--test-fire", action="store_true", help="인텔리제이 연동 검증용 샘플 BUY 시그널 즉시 발송")
    parser.add_argument("--test-sell", action="store_true", help="인텔리제이 연동 검증용 긴급 악재 SELL 시그널 즉시 발송")
    parser.add_argument("--ticker", type=str, default="NVDA", help="--test-fire / --test-sell 시 사용할 종목 티커")
    parser.add_argument("--scan-all-50", action="store_true", help="시총 50위 종목들을 하나씩 전체 순환 스캔")
    args = parser.parse_args()

    if args.test_sell:
        fire_sample_sell_signal(args.ticker if args.ticker != "NVDA" else "AAPL")
        return

    if args.test_fire:
        fire_sample_buy_signal(args.ticker)
        return

    print("==================================================")
    print(" 🇺🇸 미국 시총 Top 50 AI 이벤트 드리븐 트레이딩 워커")
    print(f" 유니버스: 미국 시총 상위 50개 대표 우량주 (NVDA, AAPL, MSFT, TSLA, LLY, WMT 등)")
    print(f" 모드: {'24시간 지속 모니터링 (' + str(args.interval) + '초 간격)' if args.loop else '1회 시장 종합 스캔 모드'}")
    print("==================================================")

    while True:
        if args.scan_all_50:
            print("\n>> [시총 50위 전체 순환 스캔 시작]")
            for ticker in list(TOP_50_US_STOCKS.keys())[:15]: # 주요 15개 먼저 순환
                process_single_ticker(ticker)
                time.sleep(1)
        else:
            scan_market_wide_news()

        if not args.loop:
            print("\n>> 1회 스캔이 완료되었습니다. 24시간 실시간 모니터링을 원하시면 --loop 옵션을 사용하세요.")
            break

        print(f"\n다음 시장 뉴스 스캔까지 {args.interval}초 대기 중...")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()