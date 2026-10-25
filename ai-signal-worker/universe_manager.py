import os
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

KST = timezone(timedelta(hours=9))

def get_now_kst() -> datetime:
    return datetime.now(KST)

UNIVERSE_FILE = "top100_universe.json"

# 미국 시총 상위 100대 대표 초우량/대형주 표준 마스터 리스트 (S&P 100 & Mega-Cap 100)
BASE_TOP100_MASTER: Dict[str, Dict[str, str]] = {
    # ── [빅테크 & 초대형 AI 플랫폼 Top 10] ──
    "MSFT": {"name": "마이크로소프트 (Microsoft)", "keywords": ["microsoft", "msft", "satya nadella", "azure", "copilot"], "exchange": "NASD", "rank": 1},
    "AAPL": {"name": "애플 (Apple)", "keywords": ["apple", "aapl", "iphone", "tim cook", "apple intelligence"], "exchange": "NASD", "rank": 2},
    "NVDA": {"name": "엔비디아 (NVIDIA)", "keywords": ["nvidia", "nvda", "jensen huang", "gpu", "blackwell"], "exchange": "NASD", "rank": 3},
    "GOOGL": {"name": "알파벳 구글 (Alphabet)", "keywords": ["google", "googl", "goog", "alphabet", "gemini", "sundar pichai"], "exchange": "NASD", "rank": 4},
    "AMZN": {"name": "아마존 (Amazon)", "keywords": ["amazon", "amzn", "aws", "andy jassy", "cloud"], "exchange": "NASD", "rank": 5},
    "META": {"name": "메타 (Meta Platforms)", "keywords": ["meta", "facebook", "zuckerberg", "instagram", "llama"], "exchange": "NASD", "rank": 6},
    "BRK.B": {"name": "버크셔 해서웨이 (Berkshire)", "keywords": ["berkshire", "buffett", "charlie munger"], "exchange": "NYSE", "rank": 7},
    "LLY": {"name": "일라이 릴리 (Eli Lilly)", "keywords": ["eli lilly", "lly", "mounjaro", "zepbound"], "exchange": "NYSE", "rank": 8},
    "AVGO": {"name": "브로드컴 (Broadcom)", "keywords": ["broadcom", "avgo", "hock tan"], "exchange": "NASD", "rank": 9},
    "TSLA": {"name": "테슬라 (Tesla)", "keywords": ["tesla", "tsla", "elon musk", "cybertruck", "robotaxi"], "exchange": "NASD", "rank": 10},

    # ── [금융 & 글로벌 결제 네트워크 Top 11-20] ──
    "JPM": {"name": "JP모건 체이스 (JPMorgan)", "keywords": ["jpmorgan", "jpm", "jamie dimon"], "exchange": "NYSE", "rank": 11},
    "WMT": {"name": "월마트 (Walmart)", "keywords": ["walmart", "wmt"], "exchange": "NYSE", "rank": 12},
    "V": {"name": "비자 (Visa)", "keywords": ["visa"], "exchange": "NYSE", "rank": 13},
    "XOM": {"name": "엑손 모빌 (ExxonMobil)", "keywords": ["exxon", "xom"], "exchange": "NYSE", "rank": 14},
    "MA": {"name": "마스터카드 (Mastercard)", "keywords": ["mastercard", "ma"], "exchange": "NYSE", "rank": 15},
    "UNH": {"name": "유나이티드헬스 (UnitedHealth)", "keywords": ["unitedhealth", "unh", "optum"], "exchange": "NYSE", "rank": 16},
    "ORCL": {"name": "오라클 (Oracle)", "keywords": ["oracle", "orcl", "larry ellison"], "exchange": "NYSE", "rank": 17},
    "COST": {"name": "코스트코 (Costco)", "keywords": ["costco", "cost"], "exchange": "NASD", "rank": 18},
    "HD": {"name": "홈디포 (Home Depot)", "keywords": ["home depot", "hd"], "exchange": "NYSE", "rank": 19},
    "PG": {"name": "프록터 앤 갬블 (P&G)", "keywords": ["procter & gamble", "pg"], "exchange": "NYSE", "rank": 20},

    # ── [필수소비 / 헬스케어 / 에너지 Top 21-30] ──
    "JNJ": {"name": "존슨앤드존슨 (J&J)", "keywords": ["johnson & johnson", "jnj"], "exchange": "NYSE", "rank": 21},
    "NFLX": {"name": "넷플릭스 (Netflix)", "keywords": ["netflix", "nflx", "squid game"], "exchange": "NASD", "rank": 22},
    "BAC": {"name": "뱅크오브아메리카 (BofA)", "keywords": ["bank of america", "bac"], "exchange": "NYSE", "rank": 23},
    "CRM": {"name": "세일즈포스 (Salesforce)", "keywords": ["salesforce", "crm", "agentforce"], "exchange": "NYSE", "rank": 24},
    "ABBV": {"name": "애브비 (AbbVie)", "keywords": ["abbvie", "abbv", "humira"], "exchange": "NYSE", "rank": 25},
    "CVX": {"name": "셰브론 (Chevron)", "keywords": ["chevron", "cvx"], "exchange": "NYSE", "rank": 26},
    "KO": {"name": "코카콜라 (Coca-Cola)", "keywords": ["coca-cola", "coke"], "exchange": "NYSE", "rank": 27},
    "MRK": {"name": "머크 (Merck)", "keywords": ["merck", "mrk", "keytruda"], "exchange": "NYSE", "rank": 28},
    "AMD": {"name": "AMD (초우량 반도체)", "keywords": ["amd", "lisa su", "mi300", "ryzen"], "exchange": "NASD", "rank": 29},
    "PEP": {"name": "펩시코 (PepsiCo)", "keywords": ["pepsico", "pep"], "exchange": "NASD", "rank": 30},

    # ── [산업재 / 클라우드 / 소프트웨어 Top 31-40] ──
    "LIN": {"name": "린데 (Linde)", "keywords": ["linde", "lin"], "exchange": "NASD", "rank": 31},
    "ADBE": {"name": "어도비 (Adobe)", "keywords": ["adobe", "adbe", "firefly"], "exchange": "NASD", "rank": 32},
    "TMO": {"name": "써모피셔 (Thermo Fisher)", "keywords": ["thermo fisher", "tmo"], "exchange": "NYSE", "rank": 33},
    "WFC": {"name": "웰스파고 (Wells Fargo)", "keywords": ["wells fargo", "wfc"], "exchange": "NYSE", "rank": 34},
    "CSCO": {"name": "시스코 (Cisco)", "keywords": ["cisco", "csco"], "exchange": "NASD", "rank": 35},
    "ACN": {"name": "액센츄어 (Accenture)", "keywords": ["accenture", "acn"], "exchange": "NYSE", "rank": 36},
    "MCD": {"name": "맥도날드 (McDonald's)", "keywords": ["mcdonald", "mcd"], "exchange": "NYSE", "rank": 37},
    "IBM": {"name": "IBM (글로벌 IT)", "keywords": ["ibm", "watson"], "exchange": "NYSE", "rank": 38},
    "GE": {"name": "GE 에어로스페이스 (GE)", "keywords": ["ge aerospace", "general electric", "ge"], "exchange": "NYSE", "rank": 39},
    "PM": {"name": "필립모리스 (Philip Morris)", "keywords": ["philip morris", "pm"], "exchange": "NYSE", "rank": 40},

    # ── [반도체 & 하드웨어 & 장비 Top 41-50] ──
    "QCOM": {"name": "퀄컴 (Qualcomm)", "keywords": ["qualcomm", "qcom", "snapdragon"], "exchange": "NASD", "rank": 41},
    "TXN": {"name": "텍사스 인스트루먼트 (TI)", "keywords": ["texas instruments", "txn"], "exchange": "NASD", "rank": 42},
    "ABT": {"name": "애보트 (Abbott)", "keywords": ["abbott", "abt"], "exchange": "NYSE", "rank": 43},
    "CAT": {"name": "캐터필러 (Caterpillar)", "keywords": ["caterpillar", "cat"], "exchange": "NYSE", "rank": 44},
    "DHR": {"name": "다나허 (Danaher)", "keywords": ["danaher", "dhr"], "exchange": "NYSE", "rank": 45},
    "VZ": {"name": "버라이즌 (Verizon)", "keywords": ["verizon", "vz"], "exchange": "NYSE", "rank": 46},
    "AMAT": {"name": "어플라이드 머티어리얼즈 (AMAT)", "keywords": ["applied materials", "amat"], "exchange": "NASD", "rank": 47},
    "NOW": {"name": "서비스나우 (ServiceNow)", "keywords": ["servicenow", "now"], "exchange": "NYSE", "rank": 48},
    "ISRG": {"name": "인튜이티브 서지컬 (다빈치 로봇수술)", "keywords": ["intuitive surgical", "isrg"], "exchange": "NASD", "rank": 49},
    "DIS": {"name": "월트 디즈니 (Disney)", "keywords": ["disney", "dis"], "exchange": "NYSE", "rank": 50},

    # ── [바이오 / 통신 / 모빌리티 Top 51-60] ──
    "PFE": {"name": "화이자 (Pfizer)", "keywords": ["pfizer", "pfe"], "exchange": "NYSE", "rank": 51},
    "INTU": {"name": "인튜이트 (Intuit)", "keywords": ["intuit", "intu", "turbotax"], "exchange": "NASD", "rank": 52},
    "CMCSA": {"name": "컴캐스트 (Comcast)", "keywords": ["comcast", "cmcsa"], "exchange": "NASD", "rank": 53},
    "UBER": {"name": "우버 (Uber)", "keywords": ["uber"], "exchange": "NYSE", "rank": 54},
    "BKNG": {"name": "부킹 홀딩스 (Booking)", "keywords": ["booking", "bkng"], "exchange": "NASD", "rank": 55},
    "COP": {"name": "코노코필립스 (ConocoPhillips)", "keywords": ["conocophillips", "cop"], "exchange": "NYSE", "rank": 56},
    "AMGN": {"name": "암젠 (Amgen)", "keywords": ["amgen", "amgn"], "exchange": "NASD", "rank": 57},
    "NEE": {"name": "넥스트에라 에너지 (친환경 전력)", "keywords": ["nextera energy", "nee"], "exchange": "NYSE", "rank": 58},
    "LOW": {"name": "로우즈 (Lowe's)", "keywords": ["lowe's", "low"], "exchange": "NYSE", "rank": 59},
    "UNP": {"name": "유니온 퍼시픽 (미국 1위 철도)", "keywords": ["union pacific", "unp"], "exchange": "NYSE", "rank": 60},

    # ── [방산 / 금융 / 항공우주 Top 61-70] ──
    "SPGI": {"name": "S&P 글로벌 (S&P Global)", "keywords": ["s&p global", "spgi"], "exchange": "NYSE", "rank": 61},
    "HON": {"name": "허니웰 (Honeywell)", "keywords": ["honeywell", "hon"], "exchange": "NASD", "rank": 62},
    "AXP": {"name": "아메리칸 익스프레스 (Amex)", "keywords": ["american express", "axp"], "exchange": "NYSE", "rank": 63},
    "RTX": {"name": "RTX (레이시온 방산)", "keywords": ["rtx", "raytheon"], "exchange": "NYSE", "rank": 64},
    "SYK": {"name": "스트라이커 (Stryker)", "keywords": ["stryker", "syk"], "exchange": "NYSE", "rank": 65},
    "GS": {"name": "골드만삭스 (Goldman Sachs)", "keywords": ["goldman sachs", "gs"], "exchange": "NYSE", "rank": 66},
    "BLK": {"name": "블랙록 (BlackRock)", "keywords": ["blackrock", "blk"], "exchange": "NYSE", "rank": 67},
    "T": {"name": "AT&T (미국 통신)", "keywords": ["at&t"], "exchange": "NYSE", "rank": 68},
    "PLTR": {"name": "팔란티어 (Palantir)", "keywords": ["palantir", "pltr", "alex karp", "aip"], "exchange": "NYSE", "rank": 69},
    "ELV": {"name": "엘레반스 헬스 (Elevance)", "keywords": ["elevance health", "elv"], "exchange": "NYSE", "rank": 70},

    # ── [제조 / 헬스케어 / 전력 인프라 Top 71-80] ──
    "ETN": {"name": "이튼 (전력 인프라 대장주)", "keywords": ["eaton", "etn", "power grid"], "exchange": "NYSE", "rank": 71},
    "TJX": {"name": "TJX 컴퍼니 (TJ Maxx)", "keywords": ["tjx", "marshalls", "tj maxx"], "exchange": "NYSE", "rank": 72},
    "MDT": {"name": "메드트로닉 (Medtronic)", "keywords": ["medtronic", "mdt"], "exchange": "NYSE", "rank": 73},
    "PGR": {"name": "프로그레시브 (Progressive)", "keywords": ["progressive", "pgr"], "exchange": "NYSE", "rank": 74},
    "BSX": {"name": "보스턴 사이언티픽 (Boston Sci)", "keywords": ["boston scientific", "bsx"], "exchange": "NYSE", "rank": 75},
    "REGN": {"name": "리제네론 (Regeneron)", "keywords": ["regeneron", "regn"], "exchange": "NASD", "rank": 76},
    "LRCX": {"name": "램리서치 (Lam Research)", "keywords": ["lam research", "lrcx"], "exchange": "NASD", "rank": 77},
    "C": {"name": "씨티그룹 (Citigroup)", "keywords": ["citigroup", "citi"], "exchange": "NYSE", "rank": 78},
    "CB": {"name": "처브 (Chubb)", "keywords": ["chubb", "cb"], "exchange": "NYSE", "rank": 79},
    "ADI": {"name": "아나로그디바이스 (ADI)", "keywords": ["analog devices", "adi"], "exchange": "NASD", "rank": 80},

    # ── [방산 / 보안 / 반도체 부품 Top 81-90] ──
    "VRTX": {"name": "버텍스 파마슈티컬 (Vertex)", "keywords": ["vertex", "vrtx"], "exchange": "NASD", "rank": 81},
    "LMT": {"name": "록히드 마틴 (Lockheed Martin)", "keywords": ["lockheed martin", "lmt"], "exchange": "NYSE", "rank": 82},
    "SCHW": {"name": "찰스 슈왑 (Charles Schwab)", "keywords": ["charles schwab", "schw"], "exchange": "NYSE", "rank": 83},
    "CI": {"name": "시그나 (Cigna)", "keywords": ["cigna", "ci"], "exchange": "NYSE", "rank": 84},
    "MMC": {"name": "마시 앤 맥레넌 (Marsh McLennan)", "keywords": ["marsh mclennan", "mmc"], "exchange": "NYSE", "rank": 85},
    "PANW": {"name": "팔로알토 네트웍스 (Palo Alto)", "keywords": ["palo alto networks", "panw"], "exchange": "NASD", "rank": 86},
    "MU": {"name": "마이크론 (Micron)", "keywords": ["micron", "mu", "hbm"], "exchange": "NASD", "rank": 87},
    "KLAC": {"name": "KLA 코퍼레이션 (KLA)", "keywords": ["kla", "klac"], "exchange": "NASD", "rank": 88},
    "SBUX": {"name": "스타벅스 (Starbucks)", "keywords": ["starbucks", "sbux", "brian niccol"], "exchange": "NASD", "rank": 89},
    "DE": {"name": "존 디어 (John Deere)", "keywords": ["deere", "john deere", "de"], "exchange": "NYSE", "rank": 90},

    # ── [소프트웨어 / AI 인프라 / 바이오 핵심 Top 91-100] ──
    "ADP": {"name": "ADP (오토매틱 데이터)", "keywords": ["adp"], "exchange": "NASD", "rank": 91},
    "FI": {"name": "파이서브 (Fiserv)", "keywords": ["fiserv", "fi"], "exchange": "NYSE", "rank": 92},
    "MO": {"name": "알트리아 (Altria)", "keywords": ["altria", "mo"], "exchange": "NYSE", "rank": 93},
    "SO": {"name": "서던 컴퍼니 (Southern)", "keywords": ["southern company", "so"], "exchange": "NYSE", "rank": 94},
    "CRWD": {"name": "크라우드스트라이크 (CrowdStrike)", "keywords": ["crowdstrike", "crwd"], "exchange": "NASD", "rank": 95},
    "BMY": {"name": "브리스톨 마이어스 (Bristol Myers)", "keywords": ["bristol myers", "bmy"], "exchange": "NYSE", "rank": 96},
    "GILD": {"name": "길리어드 사이언스 (Gilead)", "keywords": ["gilead", "gild"], "exchange": "NASD", "rank": 97},
    "ZTS": {"name": "조에티스 (Zoetis)", "keywords": ["zoetis", "zts"], "exchange": "NYSE", "rank": 98},
    "SMCI": {"name": "슈퍼마이크로 (Supermicro)", "keywords": ["supermicro", "smci"], "exchange": "NASD", "rank": 99},
    "TSM": {"name": "TSMC (대만반도체 ADR)", "keywords": ["tsmc", "tsm", "foundry"], "exchange": "NYSE", "rank": 100}
}

def get_current_monday_cycle() -> tuple[datetime, datetime]:
    """매주 월요일 09:00 KST 기준 현재 유효한 주간 사이클의 시작일과 마감일 계산"""
    now = get_now_kst()
    # 월요일 = 0, 화 = 1, ... 일 = 6
    days_since_monday = (now.weekday() - 0) % 7
    current_monday_9am = (now - timedelta(days=days_since_monday)).replace(hour=9, minute=0, second=0, microsecond=0)

    if now < current_monday_9am:
        cycle_start = current_monday_9am - timedelta(days=7)
    else:
        cycle_start = current_monday_9am

    cycle_end = cycle_start + timedelta(days=7)
    return cycle_start, cycle_end

def load_or_update_top100_universe(force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
    """
    매주 월요일 아침 9시 기준으로 미국 시총 상위 100대 기업 유니버스를 확정하고,
    일주일 동안 변경 없이 안전하게 고정 유지하는 함수
    """
    cycle_start, cycle_end = get_current_monday_cycle()
    cycle_key = cycle_start.strftime("%Y-%m-%d 09:00:00 KST")

    # 1. 기존 캐시 파일 확인
    if not force_refresh and os.path.exists(UNIVERSE_FILE):
        try:
            with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("cycle_start") == cycle_key and len(data.get("universe", {})) == 100:
                    return data["universe"]
        except Exception:
            pass

    # 2. 신규 사이클 도래 또는 최초 초기화: 미국 시총 상위 100대 기업 저장
    save_payload = {
        "cycle_name": "미국 시총 상위 100대 기업 유니버스 (Weekly Monday 09:00 KST)",
        "cycle_start": cycle_key,
        "valid_until": cycle_end.strftime("%Y-%m-%d 09:00:00 KST"),
        "total_count": len(BASE_TOP100_MASTER),
        "updated_at": get_now_kst().strftime("%Y-%m-%d %H:%M:%S KST"),
        "universe": BASE_TOP100_MASTER
    }

    try:
        with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
            json.dump(save_payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[유니버스 파일 저장 오류]: {e}")

    return BASE_TOP100_MASTER
