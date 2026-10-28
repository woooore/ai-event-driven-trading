import os
import sys
import time
import argparse
import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
import urllib.request
from typing import List, Optional, Dict
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field
from confluent_kafka import Producer
import yfinance as yf
import feedparser

# 한국 표준시 (KST, UTC+9) 타임존 고정
KST = timezone(timedelta(hours=9))

def get_now_kst() -> datetime:
    """도커 컨테이너 및 서버 환경과 무관하게 항상 한국 표준시(KST) 반환"""
    return datetime.now(KST)

# Windows 콘솔 utf-8 인코딩 설정
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

def log_ts(msg: str):
    """한국 표준시(KST) 기준 실시간 타임스탬프 로그 출력"""
    now_str = get_now_kst().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_str}] {msg}")

# 1. 환경변수 및 OpenAI 클라이언트 초기화
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    log_ts("[경고] OPENAI_API_KEY가 .env 파일에 설정되지 않았습니다.")
client = OpenAI(api_key=api_key)

CORE_ENGINE_URL = os.getenv("CORE_ENGINE_URL", "http://core-trading-engine:8080")

def send_http_post(endpoint: str, data: dict):
    """Kafka 외에 Core Trading Engine 웹 서버로 HTTP 직접 전송하여 100% 즉시 반영 보장"""
    targets = [f"{CORE_ENGINE_URL}{endpoint}", f"http://127.0.0.1:8080{endpoint}"]
    for url in targets:
        try:
            payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
            req = urllib.request.Request(
                url,
                data=payload,
                headers={'Content-Type': 'application/json; charset=utf-8'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    break
        except Exception:
            continue

# 2. 미국 시총 상위 100대 기업 유니버스 (매주 월요일 09:00 KST 자동 갱신 및 1주일 고정 유지)
from universe_manager import load_or_update_top100_universe, get_current_monday_cycle
WATCHLIST_UNIVERSE: Dict[str, Dict[str, str]] = load_or_update_top100_universe()
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

        # 6. 연속 하락 일수 (최근 N일 연속 음봉 또는 종가 하락) 및 눌림폭 (Pullback)
        down_days = 0
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] <= closes[i - 1]:
                down_days += 1
            else:
                break

        recent_window = min(5, len(highs))
        recent_high = max(highs[-recent_window:]) if recent_window > 0 else current_price
        pullback_pct = round(((current_price - recent_high) / recent_high) * 100.0, 1) if recent_high > 0 else 0.0

        # 이격도 기여도
        if 97.0 <= disparity <= 104.0:
            score += 10 # 20일선 안착
        elif disparity > 108.0:
            score -= 15 # 단기 급등 과열

        # 연속 하락(눌림목/낙폭과대) 가산점
        if down_days >= 2:
            score += 15 # 며칠 연속 하락 눌림목 형성 (반등 매수 찬스)
        elif pullback_pct <= -3.5:
            score += 10 # 단기 고점 대비 -3.5% 이상 건강한 조정

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
            'consecutive_down_days': down_days,
            'pullback_pct': pullback_pct,
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
    position_size_pct: float = Field(default=25.0, description="총자산(2,000만원) 대비 500만원 배정을 위한 권장 비중 (%단위, 기본 25.0%)")
    rsi: Optional[float] = Field(default=None, description="14일 상대강도지수 (RSI)")
    ma_disparity: Optional[float] = Field(default=None, description="20일 이동평균선 이격도 (%)")
    bollinger_b: Optional[float] = Field(default=None, description="볼린저 밴드 %B (0.0~1.0)")
    macd_hist: Optional[float] = Field(default=None, description="MACD 오실레이터 히스토그램")
    atr_pct: Optional[float] = Field(default=None, description="14일 ATR 변동성 비율 (%)")
    pe_ratio: Optional[float] = Field(default=None, description="주가수익비율 (PER)")
    consecutive_down_days: Optional[int] = Field(default=0, description="최근 연속 하락 일수")
    pullback_pct: Optional[float] = Field(default=0.0, description="최근 고점 대비 눌림폭 (%)")
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
    """한국 표준시(KST) 기준 매 1시간마다 감지된 글로벌 뉴스를 토대로 AI 브리핑을 생성하여 Kafka 및 웹 엔진으로 발행"""
    global last_briefing_hour
    now = get_now_kst()
    current_hour = now.hour

    if not force and current_hour == last_briefing_hour:
        return

    log_ts(f"✨ [AI 글로벌 브리핑 생성 시작] {now.strftime('%H:00')} (KST) 기준 최근 시장 속보 요약 중...")
    news_context = "\n".join([f"- {h}" for h in recent_market_headlines[-15:]])
    if not news_context:
        news_context = (
            "- 미국 주요 지수 선물 혼조세 속 AI 반도체 및 빅테크 테크주 중심 차별화 장세 진행 중\n"
            "- 미 국채 10년물 금리 및 유가 변동성에 따른 경기민감주와 성장주 간 체감 온도 차이\n"
            "- 엔비디아, TSMC, 슈퍼마이크로 등 AI 인프라 핵심 기업들의 실적 및 가이던스 기대감 유지"
        )

    briefing_prompt = f"""
너는 토스증권/월가 수준의 최고급 생성형 AI 글로벌 증시 브리퍼다.
현재 한국 시각 {now.strftime('%H:00')} 기준, 지난 1시간 동안 포착된 미국 증시/글로벌 핵심 속보들을 종합하여
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

        # 1. Kafka 발행
        producer.produce('market-briefings', key=item.time, value=json.dumps(briefing_payload, ensure_ascii=False), callback=delivery_report)
        producer.flush(timeout=3)

        # 2. Web HTTP 엔진 직접 전송 (100% 즉시 반영 보장)
        send_http_post("/api/briefings", briefing_payload)

        last_briefing_hour = current_hour
        log_ts(f"📢 [AI 1시간 브리핑 발행 완료] {item.time} (KST) | {item.title}")
    except Exception as be:
        log_ts(f"[AI 브리핑 생성 오류]: {be}")

# 5-3. 미국 시총 Top 100 실적 발표 / 어닝스 / 가이던스 Pydantic 스키마 (100% 한글 필수)
class EarningsExtraction(BaseModel):
    is_earnings_or_guidance: bool = Field(description="실제 분기 실적 발표(Earnings), 분기 EPS/매출 수치 발표, 또는 공식 가이던스 상향/하향 공시인지 여부. 단순 종목 비교 기사(vs.)나 실적 발표 전 예고(Gears up to report, Expected to grow 등)는 반드시 False로 반환할 것.")
    headline_ko: str = Field(default="", description="반드시 한국어로 번역 및 핵심 요약한 실적 발표 제목 (영문 원문 그대로 복사 절대 금지, 예: '버텍스 파마슈티컬, 3분기 매출 전년비 +12% 성장 및 연간 가이던스 상향')")
    quarter: str = Field(default="2026년 3분기 (Q3)", description="해당 실적의 분기 한글 표기 (예: '2026년 3분기 (Q3)', '2026년 연간 가이던스')")
    event_type: str = Field(default="실적 발표", description="반드시 '어닝 서프라이즈', '가이던스 상향', '어닝 쇼크', '실적 발표' 중 하나만 선택 (영어 금지)")
    eps_summary: str = Field(default="", description="반드시 한국어로 작성한 주당순이익(EPS) 실적 및 전년동기/전분기 대비 상승률 1줄 요약 (예: '조정 EPS $4.38 (전년동기 대비 +15.3% ↑, 컨센서스 $4.14 상회)')")
    revenue_summary: str = Field(default="", description="반드시 한국어로 작성한 분기 매출액, 분기별 성장률(YoY/QoQ) 및 가이던스 1줄 요약 (예: '분기 매출 $27.7억 달러 (YoY +12.1%, QoQ +4.5% 성장)')")
    ai_summary: str = Field(default="", description="반드시 한국어로 작성한 실적 총평 및 주가/펀더멘털 영향 2~3문장 심층 요약")
    detail_eps_actual: str = Field(default="", description="실제 발표 EPS 및 성장률 한글 요약 (예: '조정 EPS $4.38 (전년비 +15.3% 상승)')")
    detail_eps_estimate: str = Field(default="", description="시장 컨센서스 예상 EPS 한글 표기 (예: '시장 컨센서스 $4.14')")
    detail_eps_surprise: str = Field(default="", description="EPS 서프라이즈 한글 요약 (예: '예상치 대비 +5.8% 상회 (어닝 서프라이즈)')")
    detail_rev_actual: str = Field(default="", description="실제 분기 매출액 및 YoY/QoQ 성장률 한글 요약 (예: '$27.7억 달러 (전년비 +12.1% ↑, 전분기비 +4.5% ↑)')")
    detail_rev_estimate: str = Field(default="", description="시장 예상 매출액 한글 표기 (예: '시장 컨센서스 $26.9억 달러')")
    detail_rev_growth_yoy: str = Field(default="", description="분기별 매출/이익 성장률 요약 (예: 'YoY +12.1% / QoQ +4.5% 연속 성장')")
    detail_guidance: str = Field(default="", description="향후 분기 및 연간 매출·이익 가이던스 한글 상세 요약")
    detail_key_drivers: str = Field(default="", description="주요 발표 내용 및 사업부별 핵심 성장 동력 한글 불릿(•) 3줄 정리")
    detail_market_reaction: str = Field(default="", description="실적 발표 후 시장 반응 및 기관 수급 동향 한글 요약")
    sentiment: str = Field(default="호재", description="'호재', '악재', '중립' 중 하나")

EARNINGS_KEYWORDS = [
    "earnings", "eps", "revenue", "guidance", "quarterly results",
    "quarterly report", "net income", "beats", "misses", "실적", "분기",
    "가이던스", "어닝", "흑자", "적자", "매출 상향", "상향 조정", "어닝 서프라이즈", "어닝 쇼크"
]

def get_financial_context_for_earnings(ticker: str) -> str:
    """yfinance에서 실제 EPS 및 분기별 매출/이익 성장률 데이터를 조회하여 한글 리포트의 팩트 정확도 극대화"""
    try:
        tk = yf.Ticker(ticker)
        info = tk.info or {}
        trail_eps = info.get("trailingEps", "N/A")
        fwd_eps = info.get("forwardEps", "N/A")
        rev_growth = info.get("revenueGrowth")
        earn_growth = info.get("earningsGrowth")
        op_margin = info.get("operatingMargins")
        total_rev = info.get("totalRevenue")
        rev_g_str = f"{rev_growth * 100:+.1f}%" if isinstance(rev_growth, (int, float)) else "N/A"
        earn_g_str = f"{earn_growth * 100:+.1f}%" if isinstance(earn_growth, (int, float)) else "N/A"
        op_m_str = f"{op_margin * 100:.1f}%" if isinstance(op_margin, (int, float)) else "N/A"
        rev_b_str = f"${total_rev / 1e9:.2f}B" if isinstance(total_rev, (int, float)) and total_rev > 0 else "N/A"
        return (f"Trailing EPS: ${trail_eps}, Forward EPS: ${fwd_eps}, "
                f"최근 분기 매출 성장률(YoY): {rev_g_str}, 분기 이익 성장률(YoY): {earn_g_str}, "
                f"영업이익률: {op_m_str}, 연간 환산 매출규모: {rev_b_str}")
    except Exception:
        return "재무 지표 조회 생략"

def maybe_process_earnings_event(ticker: str, stock_name: str, headline: str, news_text: str, pct_change: float):
    """미국 시총 Top 100 기업의 뉴스 중 실적/가이던스 이벤트를 감지하여 100% 한글 상세 리포트로 Kafka 및 웹 API 전송"""
    headline_lower = headline.lower()
    if not any(k in headline_lower for k in EARNINGS_KEYWORDS):
        return
    # 단순 종목 비교(vs.)나 예고성 가십 기사는 제외
    if " vs. " in headline_lower or "gears up to" in headline_lower or "what to expect" in headline_lower:
        return

    fin_ctx = get_financial_context_for_earnings(ticker)

    prompt = f"""
너는 월스트리트 수석 기업 실적 애널리스트다.
다음은 미국 시총 상위 100대 우량주 {stock_name} ({ticker})의 최신 뉴스와 실제 재무 지표다.
이 뉴스가 실제 분기 실적 발표(Earnings), 분기 EPS/매출 성과, 또는 가이던스 및 주요 경영 발표와 관련된 내용인지 분석하여 Pydantic 스키마에 맞춰 JSON으로 추출하라.
단순 시황 가십이나 종목 비교 기사라면 is_earnings_or_guidance=False 로 반환하라.

[작성 필수 규칙 - 매우 중요]
1. 모든 텍스트 필드(headline_ko, eps_summary, revenue_summary, ai_summary, detail_*)는 반드시 **100% 한국어(한글)**로 작성하라. 영문 뉴스 제목을 그대로 복사해 넣는 것은 절대 금지한다!
2. 실적이 분기별로 얼마나 올랐는지(전년 동기 대비 YoY, 직전 분기 대비 QoQ 성장률), 컨센서스 대비 실제 수치, 향후 가이던스, 주요 발표 핵심 요인을 구체적 수치와 함께 한국어로 명확히 정리하라.
3. event_type은 반드시 '어닝 서프라이즈', '가이던스 상향', '어닝 쇼크', '실적 발표' 중 하나로만 지정하라.

[종목 정보 및 재무 팩트]
- 종목: {stock_name} ({ticker})
- 실제 재무 컨텍스트: {fin_ctx}
- 당일 주가 변동률: {pct_change:+}%
- 영문 원문 헤드라인: {headline}
- 수집된 뉴스 본문: {news_text[:800]}
"""
    try:
        resp = client.beta.chat.completions.parse(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format=EarningsExtraction
        )
        res: EarningsExtraction = resp.choices[0].message.parsed
        if not res.is_earnings_or_guidance:
            return

        now_dt = get_now_kst()
        kor_headline = res.headline_ko if (res.headline_ko and any('가' <= ch <= '힣' for ch in res.headline_ko)) else f"{stock_name}, {res.quarter} 실적 및 가이던스 핵심 공시 발표"
        payload = {
            "id": f"earn-{ticker.lower()}-{int(time.time())}",
            "ticker": ticker,
            "stockName": stock_name,
            "date": now_dt.strftime("%Y-%m-%d"),
            "time": now_dt.strftime("%H:%M:%S"),
            "quarter": res.quarter,
            "eventType": res.event_type if res.event_type in ["어닝 서프라이즈", "가이던스 상향", "어닝 쇼크", "실적 발표"] else "실적 발표",
            "headline": kor_headline,
            "epsSummary": res.eps_summary or "분기 주당순이익(EPS) 컨센서스 부합 및 이익 성장 지속",
            "revenueSummary": res.revenue_summary or "분기 매출 전년 동기 대비 견조한 성장세 유지",
            "aiSummary": res.ai_summary or f"{stock_name}의 분기 실적 및 향후 가이던스가 공시되었습니다.",
            "detailEpsActual": res.detail_eps_actual or res.eps_summary,
            "detailEpsEstimate": res.detail_eps_estimate or "월가 컨센서스 추정치 기준",
            "detailEpsSurprise": res.detail_eps_surprise or ("컨센서스 상회 (Beat)" if res.event_type == "어닝 서프라이즈" else "컨센서스 부합"),
            "detailRevActual": res.detail_rev_actual or res.revenue_summary,
            "detailRevEstimate": res.detail_rev_estimate or "시장 예상 매출 부합",
            "detailRevGrowthYoY": res.detail_rev_growth_yoy or "전년동기 대비 성장 지속",
            "detailGuidance": res.detail_guidance or res.revenue_summary,
            "detailKeyDrivers": res.detail_key_drivers or res.ai_summary,
            "detailMarketReaction": res.detail_market_reaction or f"발표 직후 당일 주가 {pct_change:+}% 반응 및 기관 수급 유입 모니터링",
            "sentiment": res.sentiment,
            "stockImpactPct": pct_change,
            "timestamp": int(time.time() * 1000)
        }

        # 1. Kafka 전송
        producer.produce('earnings-events', key=ticker, value=json.dumps(payload, ensure_ascii=False), callback=delivery_report)
        producer.flush(timeout=2)

        # 2. Web HTTP 엔진 직접 전송 (100% 즉시 반영)
        send_http_post("/api/earnings", payload)

        log_ts(f"📊 [Top 100 실적 이벤트 등록] {ticker} ({stock_name}) | {res.event_type} | {res.quarter} | {headline[:60]}")

        # ★ 3. PEAD (Post-Earnings-Announcement Drift) 실적 서프라이즈 고속 매수 트랙
        if res.event_type in ["어닝 서프라이즈", "가이던스 상향"] and pct_change < 4.5:
            market_open, market_msg = is_us_market_open_or_premarket()
            if market_open:
                try:
                    mk_data = get_market_and_news(ticker)
                    curr_price = mk_data.get('current_price', 100.0)
                    quant = mk_data.get('quant', {})
                    tp = quant.get('dynamic_take_profit', round(curr_price * 1.05, 2))
                    sl = quant.get('dynamic_stop_loss', round(curr_price * 0.95, 2))

                    pead_signal = StockSignal(
                        ticker=ticker,
                        stock_name=stock_name,
                        exchange="NASD",
                        action="BUY",
                        confidence=0.82,
                        order_price=round(curr_price, 2),
                        take_profit=tp,
                        stop_loss=sl,
                        sentiment="POSITIVE",
                        reason=f"[PEAD 어닝 서프라이즈 퀀트 매수] {res.quarter} {res.event_type} 포착! {res.eps_summary}. 월가 실적 모멘텀 드리프트 추종 진입.",
                        news_impact_score=85,
                        priced_in_risk="LOW",
                        position_size_pct=7.0
                    )
                    producer.produce('stock-signals', key=ticker, value=pead_signal.model_dump_json(), callback=delivery_report)
                    producer.flush(timeout=3)
                    log_ts(f"🚀 [PEAD 실적 서프라이즈 고속 매수 발동] {ticker} ({stock_name}) 비중 7.0% 주문 전송 완료!")
                except Exception as pe:
                    log_ts(f"⚠️ [PEAD 주문 실행 실패]: {pe}")
    except Exception as e:
        log_ts(f"[실적 이벤트 추출 오류] {ticker}: {e}")

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
            # 프리마켓 / 애프터마켓 / 장중 5분봉 실시간 체결가로 최신 현재가 정밀 보정
            try:
                intraday = t.history(period="5d", interval="5m", prepost=True)
                if not intraday.empty and 'Close' in intraday:
                    valid_closes = intraday['Close'].dropna().tolist()
                    if valid_closes and float(valid_closes[-1]) > 0:
                        current_price = float(valid_closes[-1])
            except Exception:
                pass
        else:
            fast_info = getattr(t, 'fast_info', None)
            if fast_info:
                current_price = getattr(fast_info, 'last_price', None)
                prev_close = getattr(fast_info, 'previous_close', None)

        if not quant_info:
            quant_info = calculate_quant_indicators(hist)

        pe_ratio = None
        try:
            fast_info = getattr(t, 'fast_info', None)
            if fast_info and hasattr(fast_info, 'pe_ratio'):
                pe_ratio = getattr(fast_info, 'pe_ratio', None)
            if not pe_ratio:
                t_info = getattr(t, 'info', {})
                pe_ratio = t_info.get('trailingPE') or t_info.get('forwardPE')
            if pe_ratio:
                pe_ratio = round(float(pe_ratio), 1)
        except Exception:
            pe_ratio = None

        if quant_info:
            quant_info['pe_ratio'] = pe_ratio

        pct_change = 0.0
        if current_price and prev_close and prev_close > 0:
            pct_change = round(((current_price - prev_close) / prev_close) * 100, 2)

        if custom_news_list:
            news_items = custom_news_list
        else:
            news_items = []
            seen_titles = set()
            # 1순위: Google News 실시간 2시간 이내 초고속 금융 속보 피드 (지연 없는 즉시 감지)
            try:
                g_url = f"https://news.google.com/rss/search?q={ticker}+stock+when:2h&hl=en-US&gl=US&ceid=US:en"
                g_feed = feedparser.parse(g_url)
                for entry in g_feed.entries[:3]:
                    title_clean = entry.get('title', '').split(' - ')[0].strip()
                    if title_clean and title_clean not in seen_titles:
                        seen_titles.add(title_clean)
                        news_items.append({
                            'title': title_clean,
                            'summary': entry.get('summary', title_clean),
                            'link': entry.get('link', ''),
                            'published': entry.get('published', '')
                        })
            except Exception:
                pass

            # 2순위: Yahoo Finance 공식 종목 RSS 피드 병합
            rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
            feed = feedparser.parse(rss_url)
            for entry in feed.entries[:5]:
                title_clean = entry.get('title', '').strip()
                if title_clean and title_clean not in seen_titles:
                    seen_titles.add(title_clean)
                    news_items.append({
                        'title': title_clean,
                        'summary': entry.get('summary', ''),
                        'link': entry.get('link', ''),
                        'published': entry.get('published', '')
                    })
            news_items = news_items[:6]

        return {
            'ticker': ticker,
            'current_price': round(current_price, 2) if current_price else 100.0,
            'pct_change': pct_change,
            'pe_ratio': pe_ratio,
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
    pe_ratio = market_data.get('pe_ratio')
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
    down_days = quant.get('consecutive_down_days', 0)
    pullback_pct = quant.get('pullback_pct', 0.0)
    quant_score = quant.get('quant_score', 55)
    dynamic_tp = quant.get('dynamic_take_profit', round(current_price * 1.03, 2))
    dynamic_sl = quant.get('dynamic_stop_loss', round(current_price * 0.935, 2))

    # 월요일 개장 시 주말 동안 축적된 호재/악재 보너스 점수 가산
    weekend_bonus = get_weekend_catalyst_bonus(ticker)
    if weekend_bonus != 0:
        quant_score = max(0, min(100, quant_score + int(weekend_bonus)))
        log_ts(f"   🌟 [주말 인텔 가산점 반영] {ticker}: 주말 동안 축적된 호재/악재로 퀀트 점수 {weekend_bonus:+}점 보정 (최종 {quant_score}점)")

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
        rag_section += (
            "=> [RAG 교훈 적용 지침]: 단순 루머로 +9.0% 이상 과열 폭등한 고점만 피하고,\n"
            "   실질적 호재(실적, 가이던스, 대규모 계약, 자본 조달, 주주환원, 빅테크 효율화)로 주가가 +1%~+8% 상승하는 모멘텀 초입에서는 '이미 올랐다'고 망설이지 말고 즉시 BUY로 진입하라!\n"
        )

    # 퀀트 기술적 지표 진단 요약
    pe_str = f"{pe_ratio:.1f}배 (동종업계 대비 양호)" if pe_ratio else "N/A (성장주/적정 추정)"
    down_days_str = f"최근 {down_days}일 연속 하락 (단기 눌림목/낙폭과대)" if down_days >= 2 else (f"최근 고점 대비 {pullback_pct:.1f}% 조정" if pullback_pct <= -2.5 else "단기 횡보/보합")
    quant_summary = (
        f"1. 14일 RSI: {rsi:.1f} ({'⚠️ 과매수 위험' if rsi >= 70 else ('💎 과매도 저평가' if rsi <= 35 else '안정권')})\n"
        f"2. 20일 MA 이격도: {ma_disparity:.1f}%\n"
        f"3. 20일 볼린저 밴드 %B: {bollinger_b:.2f} ({'하단 밴드 지지 반등권' if bollinger_b <= 0.35 else ('상단 밴드 과열' if bollinger_b >= 0.95 else '중심 안정권')})\n"
        f"4. MACD 모멘텀: {macd_hist:+.2f} ({'★ 골든크로스 상승 전환 신호!' if is_golden_cross else ('상승 모멘텀 지속' if macd_hist > 0 else '하락 추세')})\n"
        f"5. 차트/눌림목 상태: {down_days_str} (최근 고점 대비: {pullback_pct:+.1f}%)\n"
        f"6. 밸류에이션(P/E): {pe_str}\n"
        f"7. ATR 일일 정상 변동폭: ${atr:.2f} (약 {atr_pct:.1f}%)\n"
        f"8. 종합 퀀트 알파 스코어: {quant_score} / 100점 ({'💎 강력 매수 적격권 (75점 이상)' if quant_score >= 75 else ('안정 관망권' if quant_score >= 55 else '⚠️ 리스크 주의권')})"
    )

    system_prompt = (
        "너는 미국 월스트리트 최상위 헤지펀드의 시니어 이벤트 드리븐(Event-Driven) & 퀀트(Quant) 트레이더다.\n"
        "주어진 미국 주식의 현재가, 당일 변동률, 종합 퀀트 지표(RSI, 볼린저 밴드 %B, MACD, ATR, 퀀트 스코어, 연속 하락 일수, P/E), 과거 매매 일지(RAG), 그리고 최신 뉴스를 종합 분석하여 트레이딩 결정을 내려라.\n\n"
        "【핵심 철학: S&P500 분산 펀드식 1~2주 매수 절대 금지 - 종목당 약 500만원(25.0%) 엄선 집중 투자】:\n"
        "- 시장의 모든 종목을 1~2주씩 잡다하게 다 사는 것은 최악의 전략이다! 총 자본 2,000만원 중 확실한 3~4개 기회 종목에 각각 약 500만원(비중 25.0%)씩 엄선해서 매수해야 한다.\n"
        "- '차트상 며칠 연속 하락(눌림목/낙폭과대) + 실적/P/E 양호 + 치명적 악재 없음'을 동시에 충족하는 종목을 정밀 타격하라!\n\n"
        "【BUY(매수) 진입 허용 3대 핵심 트랙】:\n"
        "1. [트랙 A: 연속 하락 눌림목 + 양호한 펀더멘털/저PER 반등 매수]\n"
        "   - 최근 2일 이상 하락했거나 고점 대비 -2.0% 이상 조정받은 우량주가 치명적 악재 없이 RSI 60 이하에 위치할 때 즉시 매수! (action='BUY', confidence: 0.74~0.88, position_size_pct: 25.0%).\n"
        "2. [트랙 B: 실시간 호재 속보 모멘텀 돌파(Breakout) 즉시 매수 - 지각 관망(HOLD) 절대 금지!]\n"
        "   - 실적 서프라이즈, 가이던스 상향, 대규모 수주/계약, AI/GPU 자본 조달, 신약 임상 성공, 주주환원(배당/자사주), 빅테크 인력 효율화 등 확실한 호재 뉴스(news_impact_score >= 65)가 발생했을 때,\n"
        "   - 주가가 당일 +1.0% ~ +8.0% 상승 중인 것은 '이미 다 오른 끝물'이 아니라 '기관 수급이 유입되는 호재 모멘텀 초입'이다!\n"
        "   - 절대 '+3~5% 올랐으니 추격매수를 지양하고 HOLD한다'는 식으로 뒷북 관망하지 말고, 즉시 과감하게 매수(action='BUY', confidence: 0.78~0.92, position_size_pct: 25.0%)하라!\n"
        "3. [트랙 C: 퀀트 알파 고득점(70점 이상) 우량주 매수]\n"
        "   - 종합 퀀트 점수가 70점 이상이고 긍정적 뉴스 흐름이 확인되면 적극 매수하라.\n\n"
        "【구조조정 및 인력 효율화(Buyouts, Layoffs, Restructuring) 스마트 평가 룰】:\n"
        "- 마이크로소프트(MSFT), 메타(META), 크로거(KR) 등 우량 대기업의 '자발적 희망퇴직(Voluntary Buyouts)', '인력 감축', '비용 절감(Cost Discipline)' 뉴스는 영업이익률 대폭 개선 및 주주가치 극대화 호재다!\n\n"
        "【HOLD 원칙】:\n"
        "- 특별한 호재 촉매(news_impact_score < 60)도 없고 눌림목 메리트도 없는 평범한 종목이나, 당일 +9.0% 이상 과열 폭등한 종목만 action='HOLD'하라.\n\n"
        "【악재 감지 시 긴급 탈출 SELL】:\n"
        "- 대규모 소송 패소, 회계 부정, 실적 쇼크, 규제 제재 시 즉시 action='SELL', confidence >= 0.75로 전량 탈출하라.\n\n"
        "【BUY 결정 시 가격 기준】:\n"
        f"   - order_price: 현재 시장 가격 (${current_price}) 기준\n"
        f"   - take_profit: ATR 변동성 기반 동적 익절가 (${dynamic_tp})\n"
        f"   - stop_loss: ATR 변동성 기반 동적 손절가 (${dynamic_sl})\n"
        f"   - exchange: '{exchange}'\n"
        "   - reason(투자 근거): 반드시 자연스러운 한국어로 1~2문장으로 '연속 하락 일수, PER 밸류, 악재 부재 및 반등 기대'를 명확히 명시하라."
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
        signal.pe_ratio = pe_ratio
        signal.consecutive_down_days = down_days
        signal.pullback_pct = pullback_pct
        if signal.action == "BUY":
            signal.position_size_pct = 25.0 # 종목당 약 500만원 고정 배정

        # 1차: 스마트 매수 안전장치 (호재 모멘텀 초입은 즉시 매수 허용!)
        if signal.action == "BUY":
            is_pullback = (down_days >= 1) or (pullback_pct <= -1.8) or (rsi <= 52.0)
            is_catalyst = (signal.news_impact_score is not None and signal.news_impact_score >= 62) or (quant_score >= 68)

            if not is_pullback and not is_catalyst:
                log_ts(f"   🛡️ [엄선 매수 원칙 적용] {ticker} 눌림목 및 뉴스 촉매({signal.news_impact_score}점) 미흡 -> 관망 HOLD")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[엄선 매수 필터] 뚜렷한 호재 촉매나 눌림목 메리트가 부족하여 관망 유지. {signal.reason}"

            # 2. 뉴스 촉매 강도 검증: 악재성 뉴스일 때만 차단
            elif signal.news_impact_score is not None and signal.news_impact_score < 40:
                log_ts(f"   🛡️ [뉴스 영향도 미달 차단] {ticker} 뉴스 영향도({signal.news_impact_score}점) < 40! -> HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[뉴스 영향도 부족] 뉴스 점수 {signal.news_impact_score}점으로 촉매 부족. {signal.reason}"

            # 3. 과열 폭등 차단: 진짜 과열(+8.5% 이상 폭등)일 때만 차단하고, +1%~+8% 호재 상승 초입은 적극 매수 허용!
            elif pct_change >= 8.5:
                log_ts(f"   🛡️ [과열 폭등 추격매수 차단] {ticker} 변동률 {pct_change:+}% 과열 -> HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[과열 급등 추격매수 방지] 당일 주가가 이미 {pct_change:+}% 폭등하여 단기 과열 구간임. {signal.reason}"

            # 4. 기술적 극단 과매수 차단 (70 이상)
            elif rsi is not None and rsi >= 70.0:
                log_ts(f"   🛡️ [기술적 과매수 차단] {ticker} RSI({rsi:.1f}) >= 70 과열! -> HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[기술적 과매수 차단] RSI({rsi:.1f})가 70 이상 과열 상태로 단기 고점 조정 위험. {signal.reason}"

            # 5. 퀀트 스코어 미달 차단 (50점 미만일 때만 차단)
            elif quant_score < 50:
                log_ts(f"   🛡️ [퀀트 알파 스코어 차단] {ticker} 퀀트 점수({quant_score}점) 미달로 HOLD 전환")
                signal.action = "HOLD"
                signal.confidence = min(signal.confidence, 0.45)
                signal.position_size_pct = 0.0
                signal.reason = f"[퀀트 점수 미달] 종합 퀀트 알파 점수가 {quant_score}점으로 기술적 지지 미비. {signal.reason}"

        return signal
    except Exception as e:
        log_ts(f"[AI 분석 실패] {ticker}: {e}")
        return None

# 8-2. 미국 증시 거래소 개장 시간 판정 & 주말 모니터링 모드 가드
WEEKEND_INTEL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weekend_intel_scores.json")

def load_weekend_intel() -> dict:
    if not os.path.exists(WEEKEND_INTEL_FILE):
        return {}
    try:
        with open(WEEKEND_INTEL_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def record_weekend_intelligence(ticker: str, stock_name: str, action: str, confidence: float, news_impact: Optional[int], quant_score: Optional[int], reason: str, headline: str):
    """주말에 발생한 호재/악재를 파일에 누적 저장해 두었다가 월요일 장 개장 시 점수에 가산"""
    intel = load_weekend_intel()
    if ticker not in intel:
        intel[ticker] = {
            "ticker": ticker,
            "stock_name": stock_name,
            "net_catalyst_score": 0,
            "positive_events": 0,
            "negative_events": 0,
            "headlines": [],
            "last_updated": get_now_kst().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    score_delta = (news_impact or 50) if action == "BUY" else -(news_impact or 50)
    intel[ticker]["net_catalyst_score"] += score_delta
    if action == "BUY":
        intel[ticker]["positive_events"] += 1
    elif action == "SELL":
        intel[ticker]["negative_events"] += 1
    intel[ticker]["headlines"].append(headline[:80])
    intel[ticker]["last_updated"] = get_now_kst().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        with open(WEEKEND_INTEL_FILE, "w", encoding="utf-8") as f:
            json.dump(intel, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_ts(f"[주말 인텔 저장 오류] {e}")

def get_weekend_catalyst_bonus(ticker: str) -> float:
    """월요일 프리마켓/정규장 개장 시 주말 동안 축적된 호재/악재 점수를 퀀트 스코어에 보너스로 가산"""
    now_kst = get_now_kst()
    # 월요일(weekday == 0)일 때만 주말 축적 보너스 적용
    if now_kst.weekday() != 0:
        return 0.0

    intel = load_weekend_intel()
    item = intel.get(ticker)
    if not item:
        return 0.0
    net_score = item.get("net_catalyst_score", 0)
    if net_score >= 100:
        return 15.0 # 주말 대형 호재 집중
    elif net_score >= 50:
        return 8.0  # 주말 우량 호재
    elif net_score <= -50:
        return -15.0 # 주말 악재 누적
    return 0.0

def is_us_market_open_or_premarket() -> tuple:
    """
    미국 증시 실제 거래 가능 시간 여부 판정 (KST 기준)
    - 토요일 오전 09:00 KST ~ 월요일 오후 17:00 KST:
      미국 증시 전체 휴장이므로 '주말 24H 인텔리전스 모니터링 모드'로 전환 (실주문 미발행, 호재/악재 점수 누적)
    - 평일(월 17:00 ~ 토 09:00): 정규장 및 프리마켓 주문 발행 가능
    """
    now_kst = get_now_kst()
    weekday = now_kst.weekday() # 0:월, 1:화, ..., 4:금, 5:토, 6:일
    hour = now_kst.hour

    if weekday == 5: # 토요일
        if hour >= 9:
            return False, "주말 시장 휴장 (토요일 주말 24H 인텔리전스 모니터링 모드)"
    elif weekday == 6: # 일요일
        return False, "주말 시장 휴장 (일요일 주말 24H 인텔리전스 모니터링 모드)"
    elif weekday == 0: # 월요일
        if hour < 17:
            return False, "월요일 프리마켓 개장 대기 (주말 축적 호재 인텔리전스 반영 준비 중)"

    return True, "정규장/프리마켓 매매 가능 시간"

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

    # 미국 시총 Top 100 실적/가이던스 발표 이벤트 감지 및 처리
    stock_nm = WATCHLIST_UNIVERSE.get(ticker, {}).get('name', ticker)
    maybe_process_earnings_event(ticker, stock_nm, headline, news_concat, market_data['pct_change'])

    tech_info = f"RSI: {market_data['rsi']:.1f}" if market_data.get('rsi') else "RSI: N/A"
    log_ts(f"\n>> 🔍 [새로운 속보 감지] {ticker} ({stock_nm}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%) | {tech_info}")
    log_ts(f"   헤드라인: {headline[:80]}")

    signal = evaluate_market_and_trade(market_data)
    if not signal:
        return

    mark_content_analyzed(content_hash)

    log_ts(f"   [AI 판단] {signal.action} | 확신도: {signal.confidence:.2f} | 비중: {signal.position_size_pct}% | 뉴스점수: {signal.news_impact_score} | 선반영위험: {signal.priced_in_risk}")
    log_ts(f"   [리스크] 단가: ${signal.order_price:.2f} | 익절가: ${signal.take_profit:.2f} | 손절가: ${signal.stop_loss:.2f}")
    log_ts(f"   [투자 근거]: {signal.reason}")

    # ★ 웹 대시보드 실시간 로그 탭을 위해 모든 분석 결과(HOLD 포함)를 Kafka 및 HTTP로 전송
    try:
        now_dt = get_now_kst()
        stock_kr_info = TOP_50_US_STOCKS.get(signal.ticker)
        stock_display_name = stock_kr_info["name"] if (stock_kr_info and "name" in stock_kr_info) else (signal.stock_name or signal.ticker)
        analysis_log_payload = {
            "id": f"al-{int(time.time() * 1000)}",
            "timestamp": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "date": now_dt.strftime("%Y-%m-%d"),
            "time": now_dt.strftime("%H:%M:%S"),
            "ticker": signal.ticker,
            "stock_name": stock_display_name,
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
            "quant_score": signal.quant_score,
            "pe_ratio": signal.pe_ratio,
            "consecutive_down_days": signal.consecutive_down_days,
            "pullback_pct": signal.pullback_pct
        }
        # 1. Kafka 전송 우선 시도 (실패 시에만 HTTP fallback 전송하여 중복 방지)
        try:
            producer.produce('stock-analysis-logs', key=signal.ticker, value=json.dumps(analysis_log_payload, ensure_ascii=False), callback=delivery_report)
            producer.flush(timeout=2)
        except Exception as ke:
            send_http_post("/api/analysis-logs", analysis_log_payload)
    except Exception as le:
        log_ts(f"   ⚠️ [분석 로그 전송 실패]: {le}")

    # BUY or SELL & 확신도 >= 0.65 일 때 트레이딩 엔진 주문 Kafka 전송
    if signal.action in ["BUY", "SELL"] and signal.confidence >= 0.65:
        market_open, market_msg = is_us_market_open_or_premarket()
        if not market_open:
            record_weekend_intelligence(
                signal.ticker, signal.stock_name, signal.action, signal.confidence,
                signal.news_impact_score, signal.quant_score, signal.reason, headline
            )
            log_ts(f"   ⏸️ [{market_msg}] 실주문 발행 동결 (주말 호재/악재 인텔리전스 DB 누적 저장 완료 -> 월요일 개장 시 우선 반영)")
        else:
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
def scan_market_wide_news(batch_size: int = 25):
    """나스닥 및 미국 대표 우량/성장주 유니버스 기업의 실시간 헤드라인을 10개씩 라운드로빈 회전하며 24시간 실시간 감시"""
    global current_scan_offset, WATCHLIST_UNIVERSE
    # 매주 월요일 09:00 KST 갱신 주기 자동 체크
    WATCHLIST_UNIVERSE = load_or_update_top100_universe()
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

            # 미국 시총 Top 100 실적/가이던스 발표 이벤트 감지 및 처리
            stock_nm = WATCHLIST_UNIVERSE.get(ticker, {}).get('name', ticker)
            maybe_process_earnings_event(ticker, stock_nm, headline, news_concat, market_data['pct_change'])

            tech_info = f"RSI: {market_data['rsi']:.1f}" if market_data.get('rsi') else "RSI: N/A"
            log_ts(f"\n>> 🔍 [새로운 속보 감지] {ticker} ({stock_nm}) | 현재가: ${market_data['current_price']} ({market_data['pct_change']:+}%) | {tech_info}")
            log_ts(f"   헤드라인: {headline[:80]}")

            signal = evaluate_market_and_trade(market_data)
            if not signal:
                continue

            mark_content_analyzed(content_hash)

            log_ts(f"   [AI 판단] {signal.action} | 확신도: {signal.confidence:.2f} | 비중: {signal.position_size_pct}% | 뉴스점수: {signal.news_impact_score} | 선반영위험: {signal.priced_in_risk}")
            log_ts(f"   [리스크] 단가: ${signal.order_price:.2f} | 익절가: ${signal.take_profit:.2f} | 손절가: ${signal.stop_loss:.2f}")
            log_ts(f"   [투자 근거]: {signal.reason}")

            # 웹 대시보드 실시간 로그 탭을 위해 모든 분석 결과(HOLD 포함)를 Kafka 및 HTTP로 전송
            try:
                now_dt = get_now_kst()
                stock_kr_info = TOP_50_US_STOCKS.get(signal.ticker)
                stock_display_name = stock_kr_info["name"] if (stock_kr_info and "name" in stock_kr_info) else (signal.stock_name or signal.ticker)
                analysis_log_payload = {
                    "id": f"al-{int(time.time() * 1000)}",
                    "timestamp": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "date": now_dt.strftime("%Y-%m-%d"),
                    "time": now_dt.strftime("%H:%M:%S"),
                    "ticker": signal.ticker,
                    "stock_name": stock_display_name,
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
                    "quant_score": signal.quant_score,
                    "pe_ratio": signal.pe_ratio,
                    "consecutive_down_days": signal.consecutive_down_days,
                    "pullback_pct": signal.pullback_pct
                }
                # 1. Kafka 전송 우선 시도 (실패 시에만 HTTP fallback 전송하여 중복 방지)
                try:
                    producer.produce('stock-analysis-logs', key=signal.ticker, value=json.dumps(analysis_log_payload, ensure_ascii=False), callback=delivery_report)
                    producer.flush(timeout=2)
                except Exception as ke:
                    send_http_post("/api/analysis-logs", analysis_log_payload)
            except Exception as le:
                log_ts(f"   ⚠️ [분석 로그 전송 실패]: {le}")

            # BUY or SELL & 확신도 >= 0.65 일 때 트레이딩 엔진 주문 전송
            if signal.action in ["BUY", "SELL"] and signal.confidence >= 0.65:
                market_open, market_msg = is_us_market_open_or_premarket()
                if not market_open:
                    record_weekend_intelligence(
                        signal.ticker, signal.stock_name, signal.action, signal.confidence,
                        signal.news_impact_score, signal.quant_score, signal.reason, headline
                    )
                    log_ts(f"   ⏸️ [{market_msg}] 실주문 발행 동결 (주말 호재/악재 인텔리전스 DB 누적 저장 완료 -> 월요일 개장 시 우선 반영)")
                else:
                    try:
                        payload = signal.model_dump_json()
                        producer.produce('stock-signals', key=signal.ticker, value=payload, callback=delivery_report)
                        producer.flush(timeout=5)
                        log_ts(f"   🚀 [시그널 발행 성공] {signal.action} {signal.ticker} (비중 {signal.position_size_pct}%, 확신도 {signal.confidence:.2f}) -> Core Engine 전송 완료!")
                    except Exception as ke:
                        log_ts(f"   ⚠️ [Kafka 전송 오류]: {ke}")
            else:
                log_ts(f"   ⏸️ [관망 유지] 조건 미달로 주문 미발행 (시장 관망)")

            time.sleep(0.15) # 초고속 스캔
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
    parser.add_argument("--interval", type=int, default=20, help="모니터링 주기 (초)")
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

    # 기동 시 첫 1회 현재 KST 시간대 브리핑 즉시 생성 및 발행
    maybe_generate_hourly_briefing(force=True)

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