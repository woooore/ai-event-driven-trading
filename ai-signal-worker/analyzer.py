import os
import sys
import time
import argparse
import hashlib
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

# 3. AI 트레이딩 판단 JSON 스키마 (Pydantic Structured Output)
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

# 4. Kafka Producer 설정
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

# 5. 종목별 시세 및 뉴스 수집기
def get_market_and_news(ticker: str, custom_news_list: Optional[List[dict]] = None):
    try:
        t = yf.Ticker(ticker)
        fast_info = getattr(t, 'fast_info', None)
        
        current_price = None
        prev_close = None
        if fast_info:
            current_price = getattr(fast_info, 'last_price', None)
            prev_close = getattr(fast_info, 'previous_close', None)
        
        if not current_price:
            hist = t.history(period="2d")
            if not hist.empty:
                current_price = float(hist['Close'].iloc[-1])
                prev_close = float(hist['Close'].iloc[0]) if len(hist) > 1 else current_price

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
            'news': news_items
        }
    except Exception as e:
        print(f"[데이터 수집 오류] {ticker}: {e}")
        return None

# 6. AI 퀀트 트레이딩 판단 모델
def evaluate_market_and_trade(market_data: dict) -> Optional[StockSignal]:
    ticker = market_data['ticker']
    current_price = market_data['current_price']
    pct_change = market_data['pct_change']
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

    system_prompt = (
        "너는 미국 월스트리트 헤지펀드의 시니어 이벤트 드리븐(Event-Driven) 퀀트 트레이더다.\n"
        "주어진 미국 주식의 현재가, 당일 변동률, 그리고 최신 뉴스를 종합 분석하여 트레이딩 결정을 내려라.\n"
        "지침:\n"
        "1. 일상적인 시황 브리핑, 단순 노이즈 기사는 엄격하게 필터링하여 action='HOLD', confidence <= 0.50으로 지정하라.\n"
        "2. 실적 서프라이즈, 대규모 수주, 혁신적 제품 발표, 규제 승인, 목표주가 대폭 상향 등 주가에 강력한 모멘텀을 줄 확실한 호재일 때만 'BUY'를 결정하라.\n"
        "3. 대규모 소송 패소, 거액 배상금/과징금 판결(예: 특허 및 기술 침해 배상 등), 실적 쇼크, 규제 당국 제재 등 주가 급락을 유발할 명백한 악재일 때는 즉시 action='SELL', confidence >= 0.75로 결정하여 포지션 긴급 탈출(Emergency Exit)을 실행하라.\n"
        "4. BUY 결정 시:\n"
        "   - order_price: 현재 시장 가격 ($" + str(current_price) + ") 기준\n"
        "   - take_profit: 약 +3.0% 수준의 빠른 단기 목표 익절가\n"
        "   - stop_loss: 약 -6.5% 수준의 안전 손절 기준가\n"
        "5. exchange는 '" + exchange + "'로 설정하라."
    )

    user_prompt = f"""
[분석 대상 종목]: {company_name} ({ticker}) [거래소: {exchange}]
[현재 시장 가격]: ${current_price} (당일 변동률: {pct_change:+}%)
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
        return response.choices[0].message.parsed
    except Exception as e:
        print(f"[AI 분석 실패] {ticker}: {e}")
        return None

# 7. 중복 기사 캐시
processed_news_hashes = set()

def process_single_ticker(ticker: str, custom_news: Optional[List[dict]] = None):
    market_data = get_market_and_news(ticker, custom_news)
    if not market_data or not market_data['news']:
        return

    news_concat = "".join([n['title'] for n in market_data['news']])
    content_hash = hashlib.md5(f"{ticker}:{news_concat}".encode('utf-8')).hexdigest()
    if content_hash in processed_news_hashes:
        return

    print(f"\n>> [속보 포착] {ticker} ({TOP_50_US_STOCKS.get(ticker, {}).get('name', ticker)}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%)")
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

# 8. 실시간 전체 시장 종합 속보 스캐너 (시총 50위 종목 순환 및 속보 감지)
def scan_market_wide_news(max_tickers_per_batch: int = 15):
    """미국 시총 상위 50대 기업의 실시간 헤드라인을 순환 스캔하여 속보 발생 종목을 AI로 즉시 분석"""
    print(f">> [미국 시총 50위 유니버스 실시간 속보 스캔 중...]")
    all_tickers = list(TOP_50_US_STOCKS.keys())
    
    # 순환 스캔 대상 (우선 핵심 15개씩 순환 스캔)
    target_batch = all_tickers[:max_tickers_per_batch]
    
    analyzed_count = 0
    for ticker in target_batch:
        try:
            market_data = get_market_and_news(ticker)
            if not market_data or not market_data['news']:
                continue
            
            # 기사 해시 체크 (이전에 분석하지 않은 새로운 기사가 있는 경우만 AI 호출)
            news_concat = "".join([n['title'] for n in market_data['news']])
            content_hash = hashlib.md5(f"{ticker}:{news_concat}".encode('utf-8')).hexdigest()
            if content_hash in processed_news_hashes:
                continue

            analyzed_count += 1
            print(f"\n>> 🔍 [새로운 속보 감지] {ticker} ({TOP_50_US_STOCKS.get(ticker, {}).get('name', ticker)}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%)")
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

# 9. 인텔리제이 연동 테스트용 가상 시그널 발송
def fire_sample_buy_signal(target_ticker: str = "NVDA"):
    info = TOP_50_US_STOCKS.get(target_ticker, {"name": target_ticker, "exchange": "NASD"})
    print(f"\n>> [테스트 모드] 실시간 {target_ticker} 현재가 조회 후 연동 검증용 BUY 시그널 생성 중...")
    market_data = get_market_and_news(target_ticker)
    live_price = market_data['current_price'] if market_data else 150.00

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
        reason=f"{info['name']} announces breakthrough enterprise growth and raised fiscal guidance."
    )
    payload = sample_signal.model_dump_json()
    producer.produce('stock-signals', key=sample_signal.ticker, value=payload, callback=delivery_report)
    producer.flush()
    print(f">> [테스트 완료] 실시간 현재가(${live_price}) 기반 {target_ticker} 매수 시그널 발행 완료!")
    print(f"   내용: {payload}")

def fire_sample_sell_signal(target_ticker: str = "AAPL"):
    info = TOP_50_US_STOCKS.get(target_ticker, {"name": target_ticker, "exchange": "NASD"})
    print(f"\n>> [테스트 모드] 실시간 {target_ticker} 긴급 탈출 SELL 시그널 생성 중...")
    market_data = get_market_and_news(target_ticker)
    live_price = market_data['current_price'] if market_data else 220.00

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
        reason=f"{info['name']} hit with multi-billion dollar patent infringement liability verdict causing immediate institutional exit."
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