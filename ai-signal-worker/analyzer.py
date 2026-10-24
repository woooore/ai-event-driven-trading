import os
import sys
import time
import argparse
import hashlib
import json
import re
from datetime import datetime
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

def log_ts(msg: str):
    """실시간 타임스탬프가 포함된 로그 출력"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_str}] {msg}")

# 1. 환경변수 및 OpenAI 클라이언트 초기화
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    log_ts("[경고] OPENAI_API_KEY가 .env 파일에 설정되지 않았습니다.")
client = OpenAI(api_key=api_key)

# 2. 나스닥 100 및 미국 대표 우량/성장주 유니버스 풀 (NASDAQ & Tech Top 100)
WATCHLIST_UNIVERSE: Dict[str, Dict[str, str]] = {
    # ── [빅테크 & 초거대 플랫폼] ──
    "NVDA": {"name": "엔비디아 (NVIDIA)", "keywords": ["nvidia", "nvda", "jensen huang", "gpu", "blackwell"], "exchange": "NASD"},
    "AAPL": {"name": "애플 (Apple)", "keywords": ["apple", "aapl", "iphone", "tim cook", "apple intelligence"], "exchange": "NASD"},
    "MSFT": {"name": "마이크로소프트 (Microsoft)", "keywords": ["microsoft", "msft", "satya nadella", "azure", "copilot"], "exchange": "NASD"},
    "AMZN": {"name": "아마존 (Amazon)", "keywords": ["amazon", "amzn", "aws", "andy jassy", "cloud"], "exchange": "NASD"},
    "GOOGL": {"name": "알파벳 구글 (Alphabet)", "keywords": ["google", "googl", "goog", "alphabet", "gemini", "sundar pichai"], "exchange": "NASD"},
    "META": {"name": "메타 (Meta Platforms)", "keywords": ["meta", "facebook", "zuckerberg", "instagram", "llama"], "exchange": "NASD"},
    "TSLA": {"name": "테슬라 (Tesla)", "keywords": ["tesla", "tsla", "elon musk", "cybertruck", "robotaxi", "fsd"], "exchange": "NASD"},
    "NFLX": {"name": "넷플릭스 (Netflix)", "keywords": ["netflix", "nflx", "streaming", "squid game"], "exchange": "NASD"},

    # ── [AI 반도체 & 장비 & 하드웨어] ──
    "AVGO": {"name": "브로드컴 (Broadcom)", "keywords": ["broadcom", "avgo", "hock tan"], "exchange": "NASD"},
    "AMD": {"name": "AMD", "keywords": ["amd", "lisa su", "mi300", "ryzen", "radeon"], "exchange": "NASD"},
    "QCOM": {"name": "퀄컴 (Qualcomm)", "keywords": ["qualcomm", "qcom", "snapdragon"], "exchange": "NASD"},
    "TSM": {"name": "TSMC", "keywords": ["tsmc", "tsm", "foundry"], "exchange": "NYSE"},
    "ASML": {"name": "ASML", "keywords": ["asml", "euv", "lithography"], "exchange": "NASD"},
    "ARM": {"name": "Arm 홀딩스", "keywords": ["arm", "arm holdings", "softbank"], "exchange": "NASD"},
    "MU": {"name": "마이크론 (Micron)", "keywords": ["micron", "mu", "hbm", "dram"], "exchange": "NASD"},
    "INTC": {"name": "인텔 (Intel)", "keywords": ["intel", "intc", "pat gelsinger"], "exchange": "NASD"},
    "AMAT": {"name": "어플라이드 머티어리얼즈", "keywords": ["applied materials", "amat"], "exchange": "NASD"},
    "LRCX": {"name": "램리서치 (Lam Research)", "keywords": ["lam research", "lrcx"], "exchange": "NASD"},
    "KLAC": {"name": "KLA", "keywords": ["kla", "klac"], "exchange": "NASD"},
    "MRVL": {"name": "마벨 (Marvell)", "keywords": ["marvell", "mrvl"], "exchange": "NASD"},
    "TXN": {"name": "텍사스 인스트루먼트", "keywords": ["texas instruments", "txn"], "exchange": "NASD"},
    "SMCI": {"name": "슈퍼마이크로 (Supermicro)", "keywords": ["supermicro", "smci"], "exchange": "NASD"},

    # ── [엔터프라이즈 AI / SaaS / 클라우드 / 보안] ──
    "PLTR": {"name": "팔란티어 (Palantir)", "keywords": ["palantir", "pltr", "alex karp", "aip"], "exchange": "NYSE"},
    "ORCL": {"name": "오라클 (Oracle)", "keywords": ["oracle", "orcl", "larry ellison"], "exchange": "NYSE"},
    "CRM": {"name": "세일즈포스 (Salesforce)", "keywords": ["salesforce", "crm", "agentforce"], "exchange": "NYSE"},
    "ADBE": {"name": "어도비 (Adobe)", "keywords": ["adobe", "adbe", "firefly"], "exchange": "NASD"},
    "NOW": {"name": "서비스나우 (ServiceNow)", "keywords": ["servicenow", "now"], "exchange": "NYSE"},
    "INTU": {"name": "인튜이트 (Intuit)", "keywords": ["intuit", "intu", "turbotax"], "exchange": "NASD"},
    "IBM": {"name": "IBM", "keywords": ["ibm", "watson"], "exchange": "NYSE"},
    "PANW": {"name": "팔로알토 네트웍스", "keywords": ["palo alto networks", "panw"], "exchange": "NASD"},
    "CRWD": {"name": "크라우드스트라이크", "keywords": ["crowdstrike", "crwd"], "exchange": "NASD"},
    "SNOW": {"name": "스노우플레이크", "keywords": ["snowflake", "snow"], "exchange": "NYSE"},
    "DDOG": {"name": "데이터독 (Datadog)", "keywords": ["datadog", "ddog"], "exchange": "NASD"},
    "MDB": {"name": "몽고DB (MongoDB)", "keywords": ["mongodb", "mdb"], "exchange": "NASD"},
    "ZS": {"name": "지스케일러 (Zscaler)", "keywords": ["zscaler", "zs"], "exchange": "NASD"},
    "CSCO": {"name": "시스코 (Cisco)", "keywords": ["cisco", "csco"], "exchange": "NASD"},

    # ── [핀테크 / 전자상거래 / 플랫폼 / 모빌리티] ──
    "SHOP": {"name": "쇼피파이 (Shopify)", "keywords": ["shopify", "shop"], "exchange": "NYSE"},
    "PYPL": {"name": "페이팔 (PayPal)", "keywords": ["paypal", "pypl"], "exchange": "NASD"},
    "COIN": {"name": "코인베이스 (Coinbase)", "keywords": ["coinbase", "coin", "bitcoin", "crypto"], "exchange": "NASD"},
    "UBER": {"name": "우버 (Uber)", "keywords": ["uber"], "exchange": "NYSE"},
    "ABNB": {"name": "에어비앤비 (Airbnb)", "keywords": ["airbnb", "abnb"], "exchange": "NASD"},
    "DASH": {"name": "도어대시 (DoorDash)", "keywords": ["doordash", "dash"], "exchange": "NASD"},
    "BKNG": {"name": "부킹 홀딩스", "keywords": ["booking", "bkng"], "exchange": "NASD"},
    "MELI": {"name": "메르카도리브레", "keywords": ["mercadolibre", "meli"], "exchange": "NASD"},
    "PDD": {"name": "핀둬둬 (PDD/Temu)", "keywords": ["pdd", "temu", "pinduoduo"], "exchange": "NASD"},

    # ── [바이오 / 헬스케어 혁신] ──
    "LLY": {"name": "일라이 릴리 (Eli Lilly)", "keywords": ["eli lilly", "lly", "mounjaro", "zepbound"], "exchange": "NYSE"},
    "ISRG": {"name": "인튜이티브 서지컬", "keywords": ["intuitive surgical", "isrg", "da vinci"], "exchange": "NASD"},
    "VRTX": {"name": "버텍스 (Vertex)", "keywords": ["vertex", "vrtx"], "exchange": "NASD"},
    "REGN": {"name": "리제네론 (Regeneron)", "keywords": ["regeneron", "regn"], "exchange": "NASD"},
    "MRNA": {"name": "모더나 (Moderna)", "keywords": ["moderna", "mrna"], "exchange": "NASD"},
    "GILD": {"name": "길리어드 (Gilead)", "keywords": ["gilead", "gild"], "exchange": "NASD"},
    "AMGN": {"name": "암젠 (Amgen)", "keywords": ["amgen", "amgn"], "exchange": "NASD"},
    "UNH": {"name": "유나이티드헬스", "keywords": ["unitedhealth", "unh"], "exchange": "NYSE"},
    "TMO": {"name": "써모피셔", "keywords": ["thermo fisher", "tmo"], "exchange": "NYSE"},

    # ── [대표 소비재 / 산업 / 에너지] ──
    "COST": {"name": "코스트코 (Costco)", "keywords": ["costco", "cost"], "exchange": "NASD"},
    "WMT": {"name": "월마트 (Walmart)", "keywords": ["walmart", "wmt"], "exchange": "NYSE"},
    "SBUX": {"name": "스타벅스 (Starbucks)", "keywords": ["starbucks", "sbux"], "exchange": "NASD"},
    "LULU": {"name": "룰루레몬 (Lululemon)", "keywords": ["lululemon", "lulu"], "exchange": "NASD"},
    "DIS": {"name": "디즈니 (Disney)", "keywords": ["disney", "dis"], "exchange": "NYSE"},
    "JPM": {"name": "JP모건 (JPMorgan)", "keywords": ["jpmorgan", "jpm"], "exchange": "NYSE"},
    "V": {"name": "비자 (Visa)", "keywords": ["visa"], "exchange": "NYSE"},
    "MA": {"name": "마스터카드 (Mastercard)", "keywords": ["mastercard"], "exchange": "NYSE"},
    "CAT": {"name": "캐터필러 (Caterpillar)", "keywords": ["caterpillar", "cat"], "exchange": "NYSE"},
    "GE": {"name": "GE 에어로스페이스", "keywords": ["ge aerospace", "ge"], "exchange": "NYSE"}
}

# 기존 코드와의 하위 호환성 별칭
TOP_50_US_STOCKS = WATCHLIST_UNIVERSE

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

# 4. 월가 헤지펀드 & 로보어드바이저 퀀트 기술적 지표 종합 계산기
def calculate_quant_indicators(hist) -> dict:
    """
    1. 14일 Wilder's Smoothing RSI
    2. 20일 이동평균선 이격도 (%)
    3. 20일 볼린저 밴드 (%B 및 Bandwidth)
    4. MACD (12, 26, 9) & Histogram (골든크로스/데드크로스)
    5. ATR (Average True Range, 14일 변동성 지표 및 변동폭 비율 %)
    6. 복합 퀀트 알파 스코어 (Quant Alpha Score: 0~100점)
    """
    default_res = {
        'rsi': 50.0,
        'ma_disparity': 100.0,
        'bollinger_b': 0.50,
        'bandwidth': 10.0,
        'macd_hist': 0.0,
        'is_golden_cross': False,
        'atr': 2.0,
        'atr_pct': 2.0,
        'quant_score': 55,
        'dynamic_take_profit': 0.0,
        'dynamic_stop_loss': 0.0
    }
    if hist is None or hist.empty or len(hist) < 15:
        return default_res

    try:
        closes = [float(x) for x in hist['Close'].tolist()]
        highs = [float(x) for x in hist['High'].tolist()]
        lows = [float(x) for x in hist['Low'].tolist()]
        n = len(closes)
        current_price = closes[-1]

        # 1. 14일 Wilder's RSI
        diffs = [closes[i] - closes[i - 1] for i in range(1, n)]
        gains = [d if d > 0 else 0.0 for d in diffs]
        losses = [-d if d < 0 else 0.0 for d in diffs]
        period_rsi = 14
        avg_gain = sum(gains[:period_rsi]) / period_rsi
        avg_loss = sum(losses[:period_rsi]) / period_rsi
        for i in range(period_rsi, len(diffs)):
            avg_gain = (avg_gain * (period_rsi - 1) + gains[i]) / period_rsi
            avg_loss = (avg_loss * (period_rsi - 1) + losses[i]) / period_rsi
        rsi = 100.0 if avg_loss == 0 else (100.0 - (100.0 / (1.0 + (avg_gain / avg_loss))))
        rsi = round(max(0.0, min(100.0, rsi)), 1)

        # 2. 20일 이동평균선 및 이격도
        ma20_len = min(20, n)
        ma20_window = closes[-ma20_len:]
        ma20 = sum(ma20_window) / ma20_len
        disparity = round((current_price / ma20) * 100.0, 1) if ma20 > 0 else 100.0

        # 3. 20일 볼린저 밴드 (%B & Bandwidth)
        variance = sum((x - ma20) ** 2 for x in ma20_window) / ma20_len
        std = variance ** 0.5
        upper_bb = ma20 + (2.0 * std)
        lower_bb = ma20 - (2.0 * std)
        bb_width = upper_bb - lower_bb
        bollinger_b = round((current_price - lower_bb) / bb_width, 2) if bb_width > 0 else 0.50
        bandwidth = round((bb_width / ma20) * 100.0, 1) if ma20 > 0 else 10.0

        # 4. MACD (12, 26, 9)
        def calc_ema(series: list, length: int) -> list:
            alpha = 2.0 / (length + 1)
            ema = [series[0]]
            for val in series[1:]:
                ema.append(alpha * val + (1 - alpha) * ema[-1])
            return ema

        ema12 = calc_ema(closes, 12)
        ema26 = calc_ema(closes, 26)
        macd_line = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
        signal_line = calc_ema(macd_line, 9)
        macd_hist_series = [m - s for m, s in zip(macd_line, signal_line)]
        macd_hist = round(macd_hist_series[-1], 2)
        is_golden_cross = len(macd_hist_series) >= 2 and (macd_hist_series[-1] > 0 and macd_hist_series[-2] <= 0)

        # 5. ATR 14 (Average True Range)
        tr_list = []
        for i in range(1, n):
            h, l, prev_c = highs[i], lows[i], closes[i - 1]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            tr_list.append(tr)
        atr_window = tr_list[-min(14, len(tr_list)):]
        atr = sum(atr_window) / len(atr_window) if atr_window else (current_price * 0.02)
        atr = max(0.1, round(atr, 2))
        atr_pct = round((atr / current_price) * 100.0, 1)

        # 동적 익절/손절선 (ATR 변동성 기반 동적 리스크 관리)
        dynamic_tp = round(current_price + (1.2 * atr), 2)
        dynamic_sl = round(current_price - (1.8 * atr), 2)

        # 6. 복합 퀀트 알파 스코어 (Quant Score: 0 ~ 100점)
        score = 50

        # RSI 기여도
        if 40.0 <= rsi <= 55.0:
            score += 15 # 안정 상승 초입
        elif 30.0 <= rsi < 40.0:
            score += 12 # 과매도 저평가 반등권
        elif rsi < 30.0:
            score += 10 # 극단적 투매 Mean-Reversion
        elif 55.0 < rsi < 70.0:
            score += 5  # 완만한 상승세
        elif rsi >= 70.0:
            score -= 25 # 과매수 과열 패널티

        # 볼린저 밴드 %B 기여도
        if 0.05 <= bollinger_b <= 0.35:
            score += 15 # 하단 밴드 지지 후 반등
        elif 0.35 < bollinger_b <= 0.70:
            score += 8  # 밴드 중심 안정권
        elif bollinger_b > 0.95:
            score -= 20 # 상단 밴드 돌파 과열

        # MACD 기여도
        if is_golden_cross:
            score += 15 # 골든크로스 상승 전환!
        elif macd_hist > 0:
            score += 8  # 상승 모멘텀 유지
        else:
            score -= 8  # 하락 모멘텀

        # 이격도 기여도
        if 97.0 <= disparity <= 104.0:
            score += 10 # 20일선 안착
        elif disparity > 108.0:
            score -= 15 # 단기 급등 과열

        final_quant_score = max(0, min(100, score))

        return {
            'rsi': rsi,
            'ma_disparity': disparity,
            'bollinger_b': bollinger_b,
            'bandwidth': bandwidth,
            'macd_hist': macd_hist,
            'is_golden_cross': is_golden_cross,
            'atr': atr,
            'atr_pct': atr_pct,
            'quant_score': final_quant_score,
            'dynamic_take_profit': dynamic_tp,
            'dynamic_stop_loss': dynamic_sl
        }
    except Exception as e:
        print(f"[퀀트 지표 계산 오류]: {e}")
        return default_res

# 5. AI 트레이딩 판단 JSON 스키마 (Pydantic Structured Output)
class StockSignal(BaseModel):
    ticker: str = Field(description="미국 주식 티커 심볼 (예: NVDA, AAPL, TSLA, MSFT)")
    stock_name: str = Field(description="기업명 (예: NVIDIA Corporation)")
    exchange: str = Field(description="거래소 코드 (NASD, NYSE, AMEX)")
    action: str = Field(description="트레이딩 판단 액션: BUY, SELL, HOLD 중 하나")
    confidence: float = Field(description="AI 판단의 확신도 점수 (0.0 ~ 1.0). 단순 노이즈는 0.5 미만")
    order_price: float = Field(description="현재가 기반 진입 권장 단가 (USD)")
    stop_loss: float = Field(description="ATR 변동성 기반 동적 손절 기준가 (USD)")
    take_profit: float = Field(description="ATR 변동성 기반 동적 목표 익절 기준가 (USD)")
    sentiment: str = Field(description="POSITIVE, NEGATIVE, NEUTRAL 중 하나")
    reason: str = Field(description="AI의 핵심 투자 논거 및 매매 근거 요약 (1~2문장)")
    news_impact_score: int = Field(default=50, description="뉴스 촉매의 실질적 영향력 점수 (0~100점). 단순 소음/의견은 40점 이하, 확실한 실적/수주 팩트는 70점 이상")
    priced_in_risk: str = Field(default="LOW", description="선반영(Priced-in) 및 '뉴스에 팔아라' 위험도: LOW, MEDIUM, HIGH 중 하나")
    position_size_pct: float = Field(default=5.0, description="총자산(2,000만원) 대비 권장 매수 비중 (%단위, 3.0 ~ 15.0%). 확신도와 뉴스 영향도, 변동성에 따라 결정")
    rsi: Optional[float] = Field(default=None, description="14일 상대강도지수 (RSI)")
    ma_disparity: Optional[float] = Field(default=None, description="20일 이동평균선 이격도 (%)")
    bollinger_b: Optional[float] = Field(default=None, description="볼린저 밴드 %B (0.0~1.0)")
    macd_hist: Optional[float] = Field(default=None, description="MACD 오실레이터 히스토그램")
    atr_pct: Optional[float] = Field(default=None, description="14일 ATR 변동성 비율 (%)")
    quant_score: Optional[int] = Field(default=None, description="종합 퀀트 알파 스코어 (0~100점)")

# 5-2. 1시간 단위 AI 글로벌 시장 브리핑 Pydantic 스키마
class MarketBriefingItem(BaseModel):
    time: str = Field(description="1시간 단위 시각 (예: 06:00, 05:00, 20:00 등)")
    title: str = Field(description="글로벌 시장 핵심 1줄 헤드라인 (예: 금리 상승에 다우 약세, 반도체만 버틴 혼조 마감)")
    bullets: List[str] = Field(description="시장 요약 핵심 3문장 불릿 (친근하고 자연스러운 '~했어요', '~나왔어요' 말투)")
    category: str = Field(default="혼조", description="호재, 악재, 혼조 중 하나")

# 6. Kafka Producer 설정
kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
conf = {
    'bootstrap.servers': kafka_servers,
    'client.id': 'ai-signal-worker'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        log_ts(f"⚠️ [Kafka 전송 실패]: {err}")
    else:
        pass # 정상 전송 시 로그 간소화

last_briefing_hour = -1
recent_market_headlines: List[str] = []

def record_headline_for_briefing(headline: str):
    if headline and headline not in recent_market_headlines:
        recent_market_headlines.append(headline)
        if len(recent_market_headlines) > 30:
            recent_market_headlines.pop(0)

def maybe_generate_hourly_briefing(force: bool = False):
    """매 1시간마다 감지된 글로벌 뉴스를 토대로 1시간 단위 AI 브리핑을 생성하여 Kafka로 발행"""
    global last_briefing_hour
    now = datetime.now()
    current_hour = now.hour

    if not force and current_hour == last_briefing_hour:
        return

    if not recent_market_headlines and not force:
        return

    log_ts(f"✨ [AI 글로벌 브리핑 생성 시작] {now.strftime('%H:00')} 기준 최근 {len(recent_market_headlines)}건의 시장 속보 요약 중...")
    news_context = "\n".join([f"- {h}" for h in recent_market_headlines[-15:]])
    if not news_context:
        news_context = "- 미국 주요 증시 혼조세 속 AI 및 반도체 테크주 중심 차별화 장세 진행 중"

    briefing_prompt = f"""
너는 토스증권/월가 수준의 최고급 생성형 AI 글로벌 증시 브리퍼다.
현재 시각 {now.strftime('%H:00')} 기준, 지난 1시간 동안 포착된 미국 증시/글로벌 핵심 속보들을 종합하여
사용자에게 친근하고 직관적인 '1시간 단위 AI 브리핑'을 1개 작성하라.

[작성 규칙]:
1. title: 1줄의 핵심 헤드라인 (예: '금리 상승에 다우 약세, 반도체만 버틴 혼조 마감', '다우 1%대 하락 속 SOX 상승, 금리·유가 부담에 경기민감주 눌림')
2. bullets: 반드시 3개의 명확한 요약 문장 리스트
   - 각 문장은 자연스럽고 친근한 '~했어요', '~나왔어요', '~눌렀어요', '~지키고 있어요' 어미로 끝낼 것.
   - 첫 번째 줄: 시장 전체 분위기나 주요 지수 동향
   - 두 번째 줄: 금리, 유가, 달러 또는 매크로/섹터 부담 요인
   - 세 번째 줄: 반도체/AI/빅테크 등 차별화 종목이나 특징주 흐름
3. category: '호재', '악재', '혼조' 중 택 1

[최근 1시간 시장 속보들]:
{news_context}
"""
    try:
        resp = client.beta.chat.completions.parse(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": briefing_prompt}],
            response_format=MarketBriefingItem
        )
        item: MarketBriefingItem = resp.choices[0].message.parsed
        item.time = now.strftime("%H:00")

        briefing_payload = {
            "id": f"b-{int(time.time())}",
            "date": now.strftime("%Y. %m. %d."),
            "time": item.time,
            "title": item.title,
            "bullets": item.bullets,
            "category": item.category,
            "timestamp": int(time.time() * 1000)
        }

        producer.produce('market-briefings', key=item.time, value=json.dumps(briefing_payload, ensure_ascii=False), callback=delivery_report)
        producer.flush(timeout=5)
        last_briefing_hour = current_hour
        log_ts(f"📢 [AI 1시간 브리핑 발행 완료] {item.time} | {item.title}")
    except Exception as be:
        log_ts(f"[AI 브리핑 생성 오류]: {be}")

# 7. 종목별 시세, 기술적 지표 및 뉴스 수집기
def get_market_and_news(ticker: str, custom_news_list: Optional[List[dict]] = None):
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="1mo")

        current_price = None
        prev_close = None
        quant_info = None

        if not hist.empty and len(hist) >= 2:
            closes = hist['Close'].tolist()
            current_price = float(closes[-1])
            prev_close = float(closes[-2])
            quant_info = calculate_quant_indicators(hist)
        else:
            fast_info = getattr(t, 'fast_info', None)
            if fast_info:
                current_price = getattr(fast_info, 'last_price', None)
                prev_close = getattr(fast_info, 'previous_close', None)

        if not quant_info:
            quant_info = calculate_quant_indicators(hist)

        pct_change = 0.0
        if current_price and prev_close and prev_close > 0:
            pct_change = round(((current_price - prev_close) / prev_close) * 100, 2)

        if custom_news_list:
            news_items = custom_news_list
        else:
            rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
            feed = feedparser.parse(rss_url)
            news_items = []
            for entry in feed.entries[:5]: # 최근 5개 뉴스
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
            'quant': quant_info,
            'news': news_items
        }
    except Exception as e:
        print(f"[데이터 수집 오류] {ticker}: {e}")
        return None

# 8. AI 퀀트 트레이딩 판단 모델 (RAG 매매 일지 + 퀀트 지표 + 기술적 안전장치)
def evaluate_market_and_trade(market_data: dict) -> Optional[StockSignal]:
    ticker = market_data['ticker']
    current_price = market_data['current_price']
    pct_change = market_data['pct_change']
    quant = market_data.get('quant', {})
    news_items = market_data['news']

    if not news_items:
        return None

    rsi = quant.get('rsi', 50.0)
    ma_disparity = quant.get('ma_disparity', 100.0)
    bollinger_b = quant.get('bollinger_b', 0.50)
    macd_hist = quant.get('macd_hist', 0.0)
    is_golden_cross = quant.get('is_golden_cross', False)
    atr = quant.get('atr', round(current_price * 0.02, 2))
    atr_pct = quant.get('atr_pct', 2.0)
    quant_score = quant.get('quant_score', 55)
    dynamic_tp = quant.get('dynamic_take_profit', round(current_price * 1.03, 2))
    dynamic_sl = quant.get('dynamic_stop_loss', round(current_price * 0.935, 2))

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

    # 퀀트 기술적 지표 진단 요약
    quant_summary = (
        f"1. 14일 RSI: {rsi:.1f} ({'⚠️ 과매수 위험' if rsi >= 70 else ('💎 과매도 저평가' if rsi <= 35 else '안정권')})\n"
        f"2. 20일 MA 이격도: {ma_disparity:.1f}%\n"
        f"3. 20일 볼린저 밴드 %B: {bollinger_b:.2f} ({'하단 밴드 지지 반등권' if bollinger_b <= 0.35 else ('상단 밴드 과열' if bollinger_b >= 0.95 else '중심 안정권')})\n"
        f"4. MACD 모멘텀: {macd_hist:+.2f} ({'★ 골든크로스 상승 전환 신호!' if is_golden_cross else ('상승 모멘텀 지속' if macd_hist > 0 else '하락 추세')})\n"
        f"5. ATR 일일 정상 변동폭: ${atr:.2f} (약 {atr_pct:.1f}%)\n"
        f"6. 종합 퀀트 알파 스코어: {quant_score} / 100점 ({'💎 강력 매수 적격권 (75점 이상)' if quant_score >= 75 else ('안정 관망권' if quant_score >= 55 else '⚠️ 리스크 주의권')})"
    )

    system_prompt = (
        "너는 미국 월스트리트 최상위 헤지펀드의 시니어 이벤트 드리븐(Event-Driven) & 퀀트(Quant) 트레이더다.\n"
        "주어진 미국 주식의 현재가, 당일 변동률, 종합 퀀트 지표(RSI, 볼린저 밴드 %B, MACD, ATR, 퀀트 스코어), 과거 실패 매매 일지(RAG), 그리고 최신 뉴스를 종합 분석하여 트레이딩 결정을 내려라.\n\n"
        "【핵심 매매 원칙: 팩트 호재 우선 & 유연한 분할 매수 집행】:\n"
        "- 볼린저 밴드 하단이나 과매도 지표만 보고 매수하는 기술적 단독 매수는 금지된다. 반드시 실질적인 펀더멘털 뉴스 팩트 촉매가 결합되어야 한다.\n"
        "- 반대로, 구체적인 실적 서프라이즈, 매출/이익 가이던스 상향(예: 슈퍼마이크로 40억 달러 매출 상향 등), 대규모 공급/수주 계약 체결 등 명확한 팩트 호재가 포착되면, 지나치게 '선반영'을 핑계로 매수를 망설이지 말라!\n"
        "- 확실한 팩트가 있는 우량주는 호재 발표 후 모멘텀을 타고 추가 상승하는 경향이 강하므로, 당일 주가가 +4.5% 이상 폭등하지 않았다면 적극적으로 분할 매수(action='BUY', confidence >= 0.75, position_size_pct: 4.0~10.0%) 결정을 내려라.\n\n"
        "【필수 매매 규칙 & 손실 방어 원칙】:\n"
        "1. [뉴스 촉매 영향도 평가 (news_impact_score: 0~100)]:\n"
        "   - 단순 시황 중계, 애널리스트 의견, 근거 없는 루머는 50점 이하.\n"
        "   - 구체적인 실적 호조, 대형 수주/공급 계약, 규제 승인 등 실질적 펀더멘털 변화는 65점 이상.\n"
        "   - ★ BUY 액션은 news_impact_score >= 60 이상일 때 허용된다.\n"
        "2. [선반영(Priced-in) 평가 (priced_in_risk: LOW / MEDIUM / HIGH)]:\n"
        "   - 이미 당일 주가가 +4.5% 이상 급등했거나, 수일 전부터 시장에 완전히 퍼져 차익실현 매물 출회가 임박한 경우에만 priced_in_risk='HIGH'로 지정하고 HOLD하라.\n"
        "   - 신선한 호재이거나 당일 주가 상승폭이 완만한 경우(+4.5% 미만)는 LOW 또는 MEDIUM으로 평가하고 적극 분할 매수하라.\n"
        "3. [기술적 과매수 기준]:\n"
        "   - RSI(14)가 76.0 이상인 극단적 과열 구간이거나 볼린저 밴드 상단 완전 이탈(%B >= 1.05) 시에만 추격 매수를 자제하고 HOLD하라. (RSI 65~75는 강한 모멘텀 구간으로 팩트 호재 시 매수 가능)\n"
        "4. [비중 배분 산정 (position_size_pct: 3.0 ~ 15.0%)]:\n"
        "   - 총 시드 2,000만원 기준 종목당 권장 투자 비중.\n"
        "   - 특급 팩트 호재(news_impact_score >= 80) + 양호한 기술적 위치 => 8.0 ~ 12.0%\n"
        "   - 일반 우량 호재(news_impact_score 60~79) => 4.0 ~ 7.0%\n"
        "   - 신중한 분할 진입 => 3.0 ~ 4.0%\n"
        "   - HOLD 또는 SELL일 경우 position_size_pct = 0.0\n"
        "5. [악재 감지 시 즉시 긴급 탈출 SELL]:\n"
        "   - 대규모 소송 패소, 거액 과징금, 실적 쇼크, 회계 부정, 규제 제재 시 즉시 action='SELL', confidence >= 0.75로 전량 탈출하라.\n"
        "6. BUY 결정 시 가격 기준:\n"
        f"   - order_price: 현재 시장 가격 (${current_price}) 기준\n"
        f"   - take_profit: ATR 변동성 기반 동적 익절가 (${dynamic_tp})\n"
        f"   - stop_loss: ATR 변동성 기반 동적 손절가 (${dynamic_sl})\n"
        f"   - exchange: '{exchange}'\n"
        "7. reason(투자 근거): 반드시 자연스러운 한국어로 1~2문장으로 뉴스 팩트, 선반영 판단, 기술적 상태를 함께 언급하여 명확히 작성하라. (절대 영어가 아닌 한국어로 작성할 것)"
    )

    user_prompt = f"""
[분석 대상 종목]: {company_name} ({ticker}) [거래소: {exchange}]
[현재 시장 가격]: ${current_price} (당일 변동률: {pct_change:+}%)

[월가 퀀트 시스템 종합 진단 지표]:
{quant_summary}

{rag_section}
[최신 뉴스 피드 (최근 5개)]:
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
        signal.bollinger_b = bollinger_b
        signal.macd_hist = macd_hist
        signal.atr_pct = atr_pct
        signal.quant_score = quant_score

        # 1차: 뉴스 팩트 촉매 및 선반영 하드 안전장치
        if signal.action == "BUY":
            # 1. 뉴스 촉매 강도 미달 차단 (최소 60점 이상 필요)
            if signal.news_impact_score is not None and signal.news_impact_score < 60:
                log_ts(f"   🛡️ [뉴스 촉매 부족 차단] {ticker} 뉴스 영향도({signal.news_impact_score}점) < 60! 단순 밴드/노이즈 매수 차단 -> HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[실질 뉴스 촉매 부족] 뉴스 영향도 {signal.news_impact_score}점으로 펀더멘털 호재 팩트 미비. {signal.reason}"

            # 2. 선반영(Priced-in) 및 '뉴스에 팔아라' 매물받이 차단 (HIGH 이거나 MEDIUM이면서 +4.5% 이상 폭등 시)
            elif signal.priced_in_risk == "HIGH" or (signal.priced_in_risk == "MEDIUM" and pct_change >= 4.5):
                log_ts(f"   🛡️ [선반영(Priced-in) 차단] {ticker} 선반영 위험({signal.priced_in_risk}, 변동률 {pct_change:+}%). 차익실현 매물받이 방지 -> HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[선반영 위험 차단] 호재가 주가에 이미 선반영되었거나 당일 급등({pct_change:+}%)으로 차익실현 위험. {signal.reason}"

            # 3. 기술적 극단 과매수 차단 (76 이상)
            elif rsi is not None and rsi >= 76.0:
                log_ts(f"   🛡️ [기술적 과매수 차단] {ticker} RSI({rsi:.1f}) >= 76 극단 과열! 세력 매물받이 방어로 HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[기술적 과매수 차단] RSI({rsi:.1f})가 76 이상 극단적 과열 상태로 단기 고점 세력 차익실현 덤핑 위험. {signal.reason}"

            # 4. 볼린저 상단 이탈 과열 차단 (%B >= 1.05)
            elif bollinger_b is not None and bollinger_b >= 1.05:
                log_ts(f"   🛡️ [볼린저 상단 과열 차단] {ticker} 볼린저 밴드 상단 이탈(%B={bollinger_b}) 과열! HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[볼린저 밴드 상단 과열 차단] 주가가 볼린저 상단을 크게 돌파(%B {bollinger_b})하여 조정 압력 우려. {signal.reason}"

            # 5. 퀀트 스코어 미달 차단
            elif quant_score < 45:
                log_ts(f"   🛡️ [퀀트 알파 스코어 차단] {ticker} 퀀트 점수({quant_score}점) 저조로 HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[퀀트 점수 미달] 종합 퀀트 알파 점수가 {quant_score}점으로 기술적 지지 미비. {signal.reason}"

        return signal
    except Exception as e:
        log_ts(f"[AI 분석 실패] {ticker}: {e}")
        return None

# 9. 중복 기사 캐시 (해시와 함께 타임스탬프 기록 -> 4시간 경과 시 만료되어 주가/지표 변화 반영 재평가 가능)
processed_news_cache: Dict[str, float] = {}
current_scan_offset = 0

def is_content_analyzed(content_hash: str, ttl_hours: float = 4.0) -> bool:
    now = time.time()
    if content_hash in processed_news_cache:
        analyzed_time = processed_news_cache[content_hash]
        if (now - analyzed_time) < (ttl_hours * 3600):
            return True # 아직 TTL 기간 내에 분석된 동일 뉴스
    return False

def mark_content_analyzed(content_hash: str):
    processed_news_cache[content_hash] = time.time()

def process_single_ticker(ticker: str, custom_news: Optional[List[dict]] = None):
    market_data = get_market_and_news(ticker, custom_news)
    if not market_data or not market_data['news']:
        return

    news_concat = "".join([n['title'] for n in market_data['news']])
    content_hash = hashlib.md5(f"{ticker}:{news_concat}".encode('utf-8')).hexdigest()
    if is_content_analyzed(content_hash):
        return

    headline = market_data['news'][0]['title']
    record_headline_for_briefing(headline)

    tech_info = f"RSI: {market_data['rsi']:.1f}" if market_data.get('rsi') else "RSI: N/A"
    log_ts(f"\n>> 🔍 [새로운 속보 감지] {ticker} ({WATCHLIST_UNIVERSE.get(ticker, {}).get('name', ticker)}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%) | {tech_info}")
    log_ts(f"   헤드라인: {headline[:80]}")

    signal = evaluate_market_and_trade(market_data)
    if not signal:
        return

    mark_content_analyzed(content_hash)

    log_ts(f"   [AI 판단] {signal.action} | 확신도: {signal.confidence:.2f} | 비중: {signal.position_size_pct}% | 뉴스점수: {signal.news_impact_score} | 선반영위험: {signal.priced_in_risk}")
    log_ts(f"   [리스크] 단가: ${signal.order_price:.2f} | 익절가: ${signal.take_profit:.2f} | 손절가: ${signal.stop_loss:.2f}")
    log_ts(f"   [투자 근거]: {signal.reason}")

    # ★ 웹 대시보드 실시간 로그 탭을 위해 모든 분석 결과(HOLD 포함)를 Kafka로 전송
    try:
        now_dt = datetime.now()
        analysis_log_payload = {
            "id": f"al-{int(time.time() * 1000)}",
            "timestamp": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "date": now_dt.strftime("%Y-%m-%d"),
            "time": now_dt.strftime("%H:%M:%S"),
            "ticker": signal.ticker,
            "stock_name": signal.stock_name,
            "action": signal.action,
            "confidence": round(signal.confidence, 2),
            "news_impact_score": signal.news_impact_score,
            "priced_in_risk": signal.priced_in_risk,
            "position_size_pct": signal.position_size_pct,
            "order_price": signal.order_price,
            "pct_change": market_data['pct_change'],
            "headline": headline,
            "reason": signal.reason,
            "rsi": signal.rsi,
            "ma_disparity": signal.ma_disparity,
            "bollinger_b": signal.bollinger_b,
            "quant_score": signal.quant_score
        }
        producer.produce('stock-analysis-logs', key=signal.ticker, value=json.dumps(analysis_log_payload, ensure_ascii=False), callback=delivery_report)
        producer.flush(timeout=3)
    except Exception as le:
        log_ts(f"   ⚠️ [분석 로그 Kafka 전송 실패]: {le}")

    # BUY or SELL & 확신도 >= 0.65 일 때 트레이딩 엔진 주문 Kafka 전송
    if signal.action in ["BUY", "SELL"] and signal.confidence >= 0.65:
        try:
            payload = signal.model_dump_json()
            producer.produce('stock-signals', key=signal.ticker, value=payload, callback=delivery_report)
            producer.flush(timeout=5)
            log_ts(f"   🚀 [시그널 발행 성공] {signal.action} {signal.ticker} (비중 {signal.position_size_pct}%, 확신도 {signal.confidence:.2f}) -> Core Engine 전송 완료!")
        except Exception as ke:
            log_ts(f"   ⚠️ [Kafka 전송 오류]: {ke}")
    else:
        log_ts(f"   ⏸️ [관망 유지] 조건 미달로 주문 미발행 (시장 관망)")

# 10. 실시간 전체 시장 종합 속보 스캐너 (나스닥 & 우량/성장주 유니버스 10개씩 라운드로빈 순환)
def scan_market_wide_news(batch_size: int = 10):
    """나스닥 및 미국 대표 우량/성장주 유니버스 기업의 실시간 헤드라인을 10개씩 라운드로빈 회전하며 24시간 실시간 감시"""
    global current_scan_offset
    all_tickers = list(WATCHLIST_UNIVERSE.keys())
    total_tickers = len(all_tickers)

    # 매 1시간마다 글로벌 AI 브리핑 자동 발행 검사
    maybe_generate_hourly_briefing()

    # 유니버스 종목을 10개씩 회전(Rolling)하면서 순환
    start_idx = current_scan_offset % total_tickers
    end_idx = start_idx + batch_size

    if end_idx <= total_tickers:
        target_batch = all_tickers[start_idx:end_idx]
    else:
        target_batch = all_tickers[start_idx:] + all_tickers[:end_idx % total_tickers]

    current_scan_offset = (current_scan_offset + batch_size) % total_tickers

    log_ts(f"\n>> 🌐 [나스닥/미국 우량 유니버스 라운드로빈 스캔 ({start_idx + 1}~{min(start_idx + batch_size, total_tickers)}/{total_tickers}번째 종목)]")
    log_ts(f"   스캔 대상: {', '.join(target_batch)}")

    analyzed_count = 0
    for ticker in target_batch:
        try:
            market_data = get_market_and_news(ticker)
            if not market_data or not market_data['news']:
                continue

            news_concat = "".join([n['title'] for n in market_data['news']])
            content_hash = hashlib.md5(f"{ticker}:{news_concat}".encode('utf-8')).hexdigest()
            if is_content_analyzed(content_hash):
                continue

            analyzed_count += 1
            headline = market_data['news'][0]['title']
            record_headline_for_briefing(headline)

            tech_info = f"RSI: {market_data['rsi']:.1f}" if market_data.get('rsi') else "RSI: N/A"
            log_ts(f"\n>> 🔍 [새로운 속보 감지] {ticker} ({WATCHLIST_UNIVERSE.get(ticker, {}).get('name', ticker)}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%) | {tech_info}")
            log_ts(f"   헤드라인: {headline[:80]}")

            signal = evaluate_market_and_trade(market_data)
            if not signal:
                continue

            mark_content_analyzed(content_hash)

            log_ts(f"   [AI 판단] {signal.action} | 확신도: {signal.confidence:.2f} | 비중: {signal.position_size_pct}% | 뉴스점수: {signal.news_impact_score} | 선반영위험: {signal.priced_in_risk}")
            log_ts(f"   [리스크] 단가: ${signal.order_price:.2f} | 익절가: ${signal.take_profit:.2f} | 손절가: ${signal.stop_loss:.2f}")
            log_ts(f"   [투자 근거]: {signal.reason}")

            # 웹 대시보드 실시간 로그 탭을 위해 모든 분석 결과(HOLD 포함)를 Kafka로 전송
            try:
                now_dt = datetime.now()
                analysis_log_payload = {
                    "id": f"al-{int(time.time() * 1000)}",
                    "timestamp": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "date": now_dt.strftime("%Y-%m-%d"),
                    "time": now_dt.strftime("%H:%M:%S"),
                    "ticker": signal.ticker,
                    "stock_name": signal.stock_name,
                    "action": signal.action,
                    "confidence": round(signal.confidence, 2),
                    "news_impact_score": signal.news_impact_score,
                    "priced_in_risk": signal.priced_in_risk,
                    "position_size_pct": signal.position_size_pct,
                    "order_price": signal.order_price,
                    "pct_change": market_data['pct_change'],
                    "headline": headline,
                    "reason": signal.reason,
                    "rsi": signal.rsi,
                    "ma_disparity": signal.ma_disparity,
                    "bollinger_b": signal.bollinger_b,
                    "quant_score": signal.quant_score
                }
                producer.produce('stock-analysis-logs', key=signal.ticker, value=json.dumps(analysis_log_payload, ensure_ascii=False), callback=delivery_report)
                producer.flush(timeout=3)
            except Exception as le:
                log_ts(f"   ⚠️ [분석 로그 Kafka 전송 실패]: {le}")

            # BUY or SELL & 확신도 >= 0.65 일 때 트레이딩 엔진 주문 전송
            if signal.action in ["BUY", "SELL"] and signal.confidence >= 0.65:
                try:
                    payload = signal.model_dump_json()
                    producer.produce('stock-signals', key=signal.ticker, value=payload, callback=delivery_report)
                    producer.flush(timeout=5)
                    log_ts(f"   🚀 [시그널 발행 성공] {signal.action} {signal.ticker} (비중 {signal.position_size_pct}%, 확신도 {signal.confidence:.2f}) -> Core Engine 전송 완료!")
                except Exception as ke:
                    log_ts(f"   ⚠️ [Kafka 전송 오류]: {ke}")
            else:
                log_ts(f"   ⏸️ [관망 유지] 조건 미달로 주문 미발행 (시장 관망)")

            time.sleep(1) # API 레이트리밋 보호
        except Exception as e:
            log_ts(f"[{ticker} 스캔 오류] {e}")

    if analyzed_count == 0:
        log_ts(f">> [스캔 결과] 해당 배치({len(target_batch)}개 종목)는 기존 분석 유효 범위 내. 다음 주기에 다음 배치 종목 스캔 대기")

# 11. 인텔리제이 연동 테스트용 가상 시그널 발송
def fire_sample_buy_signal(target_ticker: str = "NVDA"):
    info = WATCHLIST_UNIVERSE.get(target_ticker, {"name": target_ticker, "exchange": "NASD"})
    log_ts(f"\n>> [테스트 모드] 실시간 {target_ticker} 현재가 조회 후 연동 검증용 다주수 비중 BUY 시그널 생성 중...")
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
        reason=f"{info['name']} 차세대 AI 아키텍처 대규모 공급 체결 및 3분기 가이던스 서프라이즈 발표.",
        news_impact_score=88,
        priced_in_risk="LOW",
        position_size_pct=10.0, # 2,000만원의 10% (약 200만원 분량 매수)
        rsi=live_rsi,
        ma_disparity=live_disp
    )
    payload = sample_signal.model_dump_json()
    producer.produce('stock-signals', key=sample_signal.ticker, value=payload, callback=delivery_report)
    producer.flush()
    log_ts(f">> [테스트 완료] 실시간 현재가(${live_price}) & 비중(10.0%) 기반 {target_ticker} 다주수 매수 시그널 발행 완료!")
    log_ts(f"   내용: {payload}")

def fire_sample_sell_signal(target_ticker: str = "AAPL"):
    info = WATCHLIST_UNIVERSE.get(target_ticker, {"name": target_ticker, "exchange": "NASD"})
    log_ts(f"\n>> [테스트 모드] 실시간 {target_ticker} 긴급 탈출 SELL 시그널 생성 중...")
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
        reason=f"{info['name']} 독점 금지법 위반 거액 과징금 및 핵심 라이선스 효력 정지 판결로 전량 청산.",
        news_impact_score=90,
        priced_in_risk="HIGH",
        position_size_pct=0.0,
        rsi=live_rsi,
        ma_disparity=live_disp
    )
    payload = sample_signal.model_dump_json()
    producer.produce('stock-signals', key=sample_signal.ticker, value=payload, callback=delivery_report)
    producer.flush()
    log_ts(f">> [테스트 완료] {target_ticker} 악재 감지 긴급 매도(SELL) 시그널 발행 완료!")
    log_ts(f"   내용: {payload}")

# 12. 메인 실행 함수
def main():
    parser = argparse.ArgumentParser(description="나스닥 & 미국 주요 우량/성장주 AI 이벤트 드리븐 트레이딩 워커")
    parser.add_argument("--loop", action="store_true", help="24시간 실시간 무한 모니터링 실행")
    parser.add_argument("--interval", type=int, default=60, help="모니터링 주기 (초)")
    parser.add_argument("--test-fire", action="store_true", help="인텔리제이 연동 검증용 샘플 BUY 시그널 즉시 발송")
    parser.add_argument("--test-sell", action="store_true", help="인텔리제이 연동 검증용 긴급 악재 SELL 시그널 즉시 발송")
    parser.add_argument("--ticker", type=str, default="NVDA", help="--test-fire / --test-sell 시 사용할 종목 티커")
    parser.add_argument("--scan-all-universe", action="store_true", help="유니버스 전체 종목을 하나씩 순환 스캔")
    parser.add_argument("--test-briefing", action="store_true", help="1시간 단위 AI 브리핑 즉시 생성 테스트")
    args = parser.parse_args()

    if args.test_briefing:
        maybe_generate_hourly_briefing(force=True)
        return

    if args.test_sell:
        fire_sample_sell_signal(args.ticker if args.ticker != "NVDA" else "AAPL")
        return

    if args.test_fire:
        fire_sample_buy_signal(args.ticker)
        return

    log_ts("==================================================")
    log_ts(" 🇺🇸 나스닥 & 미국 성장주 유니버스 AI 이벤트 드리븐 트레이딩 워커")
    log_ts(f" 유니버스: 나스닥 100 및 주요 AI/반도체/SaaS/바이오 {len(WATCHLIST_UNIVERSE)}개 종목 풀")
    log_ts(f" 시드머니: 20,000,000 KRW (비중 기반 동적 수량 매수)")
    log_ts(f" 모드: {'24시간 지속 모니터링 (' + str(args.interval) + '초 간격)' if args.loop else '1회 시장 종합 스캔 모드'}")
    log_ts("==================================================")

    # 기동 시 첫 1회 브리핑 생성 시도
    maybe_generate_hourly_briefing(force=False)

    while True:
        if args.scan_all_universe:
            log_ts(f"\n>> [전체 유니버스 {len(WATCHLIST_UNIVERSE)}개 순환 스캔 시작]")
            for ticker in list(WATCHLIST_UNIVERSE.keys())[:20]:
                process_single_ticker(ticker)
                time.sleep(1)
        else:
            scan_market_wide_news()

        if not args.loop:
            log_ts("\n>> 1회 스캔이 완료되었습니다. 24시간 실시간 모니터링을 원하시면 --loop 옵션을 사용하세요.")
            break

        log_ts(f"\n다음 시장 뉴스 스캔까지 {args.interval}초 대기 중...")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()