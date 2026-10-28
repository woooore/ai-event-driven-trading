package com.trading.engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.EarningsEventDto;
import com.trading.engine.dto.UpcomingEarningsDto;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
@RequiredArgsConstructor
public class EarningsService {

    private final ObjectMapper objectMapper;
    private final List<EarningsEventDto> earningsEvents = new CopyOnWriteArrayList<>();
    private static final String EARNINGS_FILE = "earnings_events.json";

    @PostConstruct
    public void init() {
        loadFromFile();
        if (earningsEvents.isEmpty()) {
            initSeedEarnings();
            saveToFile();
        }
    }

    public List<EarningsEventDto> getEarnings(String filter, String dateStr) {
        String targetDigits = normalizeDate(dateStr);
        List<EarningsEventDto> list = new ArrayList<>();

        for (EarningsEventDto e : earningsEvents) {
            // 날짜 필터 (지정된 경우)
            if (dateStr != null && !dateStr.isBlank()) {
                if (!normalizeDate(e.getDate()).equals(targetDigits)) {
                    continue;
                }
            }

            // 카테고리 필터
            if (filter == null || filter.isBlank() || "ALL".equalsIgnoreCase(filter)) {
                list.add(e);
            } else if ("SURPRISE".equalsIgnoreCase(filter) && "어닝 서프라이즈".equals(e.getEventType())) {
                list.add(e);
            } else if ("GUIDANCE".equalsIgnoreCase(filter) && "가이던스 상향".equals(e.getEventType())) {
                list.add(e);
            } else if ("SHOCK".equalsIgnoreCase(filter) && "어닝 쇼크".equals(e.getEventType())) {
                list.add(e);
            }
        }

        // 최신 타임스탬프 순 정렬
        list.sort((a, b) -> Long.compare(
                b.getTimestamp() != null ? b.getTimestamp() : 0L,
                a.getTimestamp() != null ? a.getTimestamp() : 0L
        ));
        return list;
    }

    public Map<String, Object> getSummary() {
        int total = earningsEvents.size();
        int surprise = 0;
        int guidance = 0;
        int shock = 0;

        for (EarningsEventDto e : earningsEvents) {
            if ("어닝 서프라이즈".equals(e.getEventType())) surprise++;
            else if ("가이던스 상향".equals(e.getEventType())) guidance++;
            else if ("어닝 쇼크".equals(e.getEventType())) shock++;
        }

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("total", total);
        map.put("surprise", surprise);
        map.put("guidance", guidance);
        map.put("shock", shock);
        return map;
    }

    /**
     * ⏰ 향후 2주 이내 미국 시총 Top 100 기업 실적 발표 예정 리스트 (D-Day 순)
     */
    public List<UpcomingEarningsDto> getUpcomingEarnings() {
        List<UpcomingEarningsDto> list = new ArrayList<>();

        list.add(UpcomingEarningsDto.builder()
                .ticker("GOOGL")
                .stockName("알파벳 구글 (Alphabet)")
                .expectedDate("2026-10-30")
                .expectedTimeKst("05:00 KST (장 마감 후)")
                .dDay("D-1")
                .dDayNumber(1)
                .quarter("2026 Q3")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$1.85 (YoY +12.8%)")
                .revenueConsensus("$863억 달러 (YoY +14.2%)")
                .keyWatchPoints("구글 클라우드 영업이익률 확장, 제미나이 AI 검색 연계 광고 매출 성장률")
                .impliedVolatility("±6.2%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("MSFT")
                .stockName("마이크로소프트 (Microsoft)")
                .expectedDate("2026-10-30")
                .expectedTimeKst("05:15 KST (장 마감 후)")
                .dDay("D-1")
                .dDayNumber(1)
                .quarter("FY27 Q1 (2026 Q3)")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$3.10 (YoY +10.5%)")
                .revenueConsensus("$645억 달러 (YoY +14.1%)")
                .keyWatchPoints("Azure 클라우드 성장률 30% 방어 여부, AI 인프라 자본지출(CapEx) 회수율")
                .impliedVolatility("±4.8%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("AMD")
                .stockName("AMD (초우량 반도체)")
                .expectedDate("2026-10-30")
                .expectedTimeKst("05:15 KST (장 마감 후)")
                .dDay("D-1")
                .dDayNumber(1)
                .quarter("2026 Q3")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$0.92 (YoY +31.4%)")
                .revenueConsensus("$67.1억 달러 (YoY +15.7%)")
                .keyWatchPoints("MI325X AI 가속기 공급 실적, 서버용 EPYC CPU 점유율 확대 추이")
                .impliedVolatility("±8.5%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("LLY")
                .stockName("일라이 릴리 (Eli Lilly)")
                .expectedDate("2026-10-30")
                .expectedTimeKst("19:30 KST (장 시작 전)")
                .dDay("D-1")
                .dDayNumber(1)
                .quarter("2026 Q3")
                .marketTiming("장 시작 전 (BMO)")
                .epsConsensus("$1.47 (YoY +135%)")
                .revenueConsensus("$121억 달러 (YoY +27.4%)")
                .keyWatchPoints("젭바운드/마운자로 비만약 공급 병목 해소 속도, 글로벌 신약 승인 파이프라인")
                .impliedVolatility("±5.5%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("META")
                .stockName("메타 (Meta Platforms)")
                .expectedDate("2026-10-31")
                .expectedTimeKst("05:05 KST (장 마감 후)")
                .dDay("D-2")
                .dDayNumber(2)
                .quarter("2026 Q3")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$5.25 (YoY +19.6%)")
                .revenueConsensus("$402억 달러 (YoY +18.0%)")
                .keyWatchPoints("Llama AI 추천 알고리즘을 통한 광고 단가 상승, 리얼리티 랩스 손실 축소 여부")
                .impliedVolatility("±7.9%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("AAPL")
                .stockName("애플 (Apple)")
                .expectedDate("2026-10-31")
                .expectedTimeKst("05:30 KST (장 마감 후)")
                .dDay("D-2")
                .dDayNumber(2)
                .quarter("FY26 Q4 (2026 Q3)")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$1.60 (YoY +9.6%)")
                .revenueConsensus("$945억 달러 (YoY +5.6%)")
                .keyWatchPoints("아이폰 17 초기 판매 추이, 애플 인텔리전스(AI) 글로벌 롤아웃 및 서비스 부문 마진")
                .impliedVolatility("±4.2%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("AMZN")
                .stockName("아마존 (Amazon)")
                .expectedDate("2026-11-01")
                .expectedTimeKst("05:00 KST (장 마감 후)")
                .dDay("D-3")
                .dDayNumber(3)
                .quarter("2026 Q3")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$1.14 (YoY +21.3%)")
                .revenueConsensus("$1,572억 달러 (YoY +10.0%)")
                .keyWatchPoints("AWS 클라우드 매출 성장 가속화(19%+), 북미 물류 로봇 자동화로 인한 영업이익률")
                .impliedVolatility("±6.8%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("PLTR")
                .stockName("팔란티어 (Palantir)")
                .expectedDate("2026-11-05")
                .expectedTimeKst("06:05 KST (장 마감 후)")
                .dDay("D-7")
                .dDayNumber(7)
                .quarter("2026 Q3")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$0.09 (YoY +28.5%)")
                .revenueConsensus("$7.01억 달러 (YoY +25.6%)")
                .keyWatchPoints("미국 민간 AIP 부트캠프 계약 전환율, S&P500 편입 후 기관 자금 순유입")
                .impliedVolatility("±11.2%")
                .build());

        list.add(UpcomingEarningsDto.builder()
                .ticker("QCOM")
                .stockName("퀄컴 (Qualcomm)")
                .expectedDate("2026-11-07")
                .expectedTimeKst("06:00 KST (장 마감 후)")
                .dDay("D-9")
                .dDayNumber(9)
                .quarter("FY26 Q4 (2026 Q3)")
                .marketTiming("장 마감 후 (AMC)")
                .epsConsensus("$2.56 (YoY +26.7%)")
                .revenueConsensus("$99.0억 달러 (YoY +14.2%)")
                .keyWatchPoints("스냅드래곤 8 엘리트 온디바이스 AI AP 공급량, 오토모티브/IoT 매출 다변화")
                .impliedVolatility("±6.5%")
                .build());

        return list;
    }

    public synchronized void addEarningsEvent(EarningsEventDto dto) {
        if (dto == null) return;
        if (dto.getId() == null) {
            dto.setId("earn-" + System.currentTimeMillis() + "-" + (int)(Math.random() * 1000));
        }
        if (dto.getTimestamp() == null) {
            dto.setTimestamp(System.currentTimeMillis());
        }
        if (dto.getDate() == null) {
            dto.setDate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }

        // 중복 방지 (동일 티커, 동일 분기 실적이면 업데이트)
        boolean updated = false;
        for (int i = 0; i < earningsEvents.size(); i++) {
            EarningsEventDto exist = earningsEvents.get(i);
            if (exist.getTicker().equalsIgnoreCase(dto.getTicker()) &&
                Objects.equals(exist.getQuarter(), dto.getQuarter()) &&
                normalizeDate(exist.getDate()).equals(normalizeDate(dto.getDate()))) {
                earningsEvents.set(i, dto);
                updated = true;
                break;
            }
        }
        if (!updated) {
            earningsEvents.add(0, dto);
        }

        saveToFile();
        log.info(">> 📊 [미국 시총 Top 100 실적 이벤트 등록] {} ({}) - {} | {}", dto.getTicker(), dto.getStockName(), dto.getEventType(), dto.getHeadline());
    }

    private String normalizeDate(String d) {
        if (d == null || d.isBlank()) return "";
        return d.replaceAll("[^0-9]", "");
    }

    private void loadFromFile() {
        File file = new File(EARNINGS_FILE);
        if (file.exists() && file.length() > 0) {
            try {
                List<EarningsEventDto> loaded = objectMapper.readValue(file, new TypeReference<>() {});
                earningsEvents.clear();
                earningsEvents.addAll(loaded);
                log.info(">> [실적 발표 트래커] 저장된 실적 이벤트 {}건 로드 완료", earningsEvents.size());
            } catch (Exception e) {
                log.error("실적 이벤트 파일 로드 실패: {}", e.getMessage());
            }
        }
    }

    private void saveToFile() {
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(EARNINGS_FILE), earningsEvents);
        } catch (Exception e) {
            log.error("실적 이벤트 파일 저장 실패: {}", e.getMessage());
        }
    }

    /**
     * 미국 시총 상위 100대 기업 최근 핵심 실적 발표 및 상세 팩트 시드 데이터
     */
    private void initSeedEarnings() {
        String todayDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        long now = System.currentTimeMillis();

        earningsEvents.add(EarningsEventDto.builder()
                .id("earn-smci-1")
                .ticker("SMCI")
                .stockName("슈퍼마이크로 (Supermicro)")
                .date(todayDate)
                .time("20:12:00")
                .quarter("2026 연간 가이던스")
                .eventType("가이던스 상향")
                .headline("Supermicro Raises Full-Year Revenue Guidance by $4 Billion on Massive AI Server Demand")
                .epsSummary("연간 주당순이익(EPS) 추정치 $34.50로 상향 조정")
                .revenueSummary("연간 매출 가이던스 $280억 달러로 전격 상향 (+40억 달러 증액)")
                .aiSummary("글로벌 AI 데이터센터 증설에 따른 수랭식(DLC) 서버 랙 주문이 폭증하며 연간 매출을 40억 달러 전격 상향했습니다. 단순 실적 기대감을 넘어 실질적 현금 창출 팩트가 확인되었습니다.")
                .sentiment("호재")
                .stockImpactPct(+2.10)
                .timestamp(now - 3600000)
                .detailEpsActual("$34.50 (연간 전망치)")
                .detailEpsEstimate("$31.80 (기존 컨센서스)")
                .detailEpsSurprise("+8.49% Upward Revision")
                .detailRevActual("$280억 달러 (연간)")
                .detailRevEstimate("$240억 달러 (종전 가이던스)")
                .detailRevGrowthYoY("+87.5% YoY 폭발적 성장")
                .detailGuidance("수랭식 액체 냉각(DLC) AI 서버 랙 출하량 월 5,000개 돌파 전망 및 추가 생산라인 가동")
                .detailKeyDrivers("엔비디아 블랙웰 GB200 서버 랙 아키텍처 대량 수주 및 클라우드 서비스 기업들의 전력 효율화 장비 선점 경쟁")
                .detailMarketReaction("장외 시간외 거래에서 거래량 3배 급증, 애널리스트 6개사 목표주가 일제히 상향")
                .build());

        earningsEvents.add(EarningsEventDto.builder()
                .id("earn-unh-1")
                .ticker("UNH")
                .stockName("유나이티드헬스 (UnitedHealth)")
                .date(todayDate)
                .time("17:45:00")
                .quarter("2026 Q3")
                .eventType("어닝 서프라이즈")
                .headline("UnitedHealth Beats Q3 Expectations: Adjusted EPS Surges 61% YoY")
                .epsSummary("조정 EPS $7.15 (시장 예상치 $6.82 대폭 상회, +61% YoY)")
                .revenueSummary("매출 $1,008억 달러 (+9.2% YoY 시장 기대치 충족)")
                .aiSummary("보험 청구 비용 안정화와 옵텀(Optum) 헬스케어 서비스 부문의 고성장으로 분기 조정 EPS가 전년 대비 61% 증가하는 메가 어닝 서프라이즈를 달성했습니다.")
                .sentiment("호재")
                .stockImpactPct(+1.85)
                .timestamp(now - 7200000)
                .detailEpsActual("$7.15")
                .detailEpsEstimate("$6.82")
                .detailEpsSurprise("+4.84% Beat")
                .detailRevActual("$1,008억 달러")
                .detailRevEstimate("$998억 달러")
                .detailRevGrowthYoY("+9.2% YoY")
                .detailGuidance("연간 조정 EPS 가이던스 $27.50 ~ $27.75 유지 및 옵텀 부문 마진 추가 개선 전망")
                .detailKeyDrivers("메디케어 어드밴티지 손해율(MLR)이 85.2%로 예상치보다 안정화되었으며, 데이터 분석 기반 헬스케어 솔루션 매출 호조")
                .detailMarketReaction("실적 발표 직후 프리마켓 +3.2% 상승 출발, 배당 확대 및 자사주 매입 계획 동시 발표")
                .build());

        earningsEvents.add(EarningsEventDto.builder()
                .id("earn-tsm-1")
                .ticker("TSM")
                .stockName("TSMC")
                .date(todayDate)
                .time("14:30:00")
                .quarter("2026 Q3")
                .eventType("어닝 서프라이즈")
                .headline("TSMC Reports 54% Surge in Net Profit, Driven by Advanced AI Chip Packaging")
                .epsSummary("분기 순이익 3,252억 대만달러 (+54.2% YoY, 시장 컨센서스 상회)")
                .revenueSummary("3나노/5나노 첨단 공정 매출 비중 69% 돌파")
                .aiSummary("엔비디아, 애플, AMD의 선단 공정 주문이 가동률 100%를 기록하며 순이익이 54% 폭증했습니다. 4분기 매출 가이던스 역시 두 자릿수 성장을 제시했습니다.")
                .sentiment("호재")
                .stockImpactPct(+3.20)
                .timestamp(now - 14400000)
                .detailEpsActual("$1.94 (ADR 기준)")
                .detailEpsEstimate("$1.79")
                .detailEpsSurprise("+8.38% Beat")
                .detailRevActual("$235억 달러")
                .detailRevEstimate("$228억 달러")
                .detailRevGrowthYoY("+39.0% YoY")
                .detailGuidance("4분기 매출 가이던스 $261억 ~ $269억 달러 제시, CoWoS 첨단 패키징 생산 능력 2배 확장 선언")
                .detailKeyDrivers("AI 가속기 칩 수요 폭발로 3나노/5나노 웨이퍼 가동률 풀캐파 도달, 스마트폰용 고성능 AP 수주 지속")
                .detailMarketReaction("반도체 섹터 전반 랠리 촉발, 글로벌 기관 매수세 집중 유입")
                .build());

        earningsEvents.add(EarningsEventDto.builder()
                .id("earn-nflx-1")
                .ticker("NFLX")
                .stockName("넷플릭스 (Netflix)")
                .date(todayDate)
                .time("09:15:00")
                .quarter("2026 Q3")
                .eventType("어닝 서프라이즈")
                .headline("Netflix Adds 5.1M Paid Subscribers in Q3, Operating Margin Expands to 30%")
                .epsSummary("주당순이익(EPS) $5.40 (예상치 $5.12 상회)")
                .revenueSummary("매출 $98.3억 달러 (+15.0% YoY)")
                .aiSummary("유료 구독자 510만 명 순증과 광고형 요금제 침투율 확대로 영업이익률이 30%로 급등했습니다. 라이브 이벤트 중계 확대로 4분기 호실적 모멘텀이 강화되었습니다.")
                .sentiment("호재")
                .stockImpactPct(+4.80)
                .timestamp(now - 28800000)
                .detailEpsActual("$5.40")
                .detailEpsEstimate("$5.12")
                .detailEpsSurprise("+5.47% Beat")
                .detailRevActual("$98.3억 달러")
                .detailRevEstimate("$97.7억 달러")
                .detailRevGrowthYoY("+15.0% YoY")
                .detailGuidance("4분기 매출 101.4억 달러 전망 및 연간 영업이익률 27% -> 30%로 상향 제시")
                .detailKeyDrivers("광고형 요금제 가입자 전분기 대비 35% 급증, 계정 공유 금지 효과 안정화, 인기 오리지널 시리즈 라인업 안착")
                .detailMarketReaction("실적 발표 후 장외 11% 급등, 월가 애널리스트 12명 목표가 상향 조정")
                .build());

        earningsEvents.add(EarningsEventDto.builder()
                .id("earn-pltr-1")
                .ticker("PLTR")
                .stockName("팔란티어 (Palantir)")
                .date(todayDate)
                .time("11:20:00")
                .quarter("2026 Q3")
                .eventType("가이던스 상향")
                .headline("Palantir Raises Annual Revenue Guidance as US Commercial AIP Customer Count Jumps 83%")
                .epsSummary("조정 EPS $0.10 (흑자 지속 및 컨센서스 부합)")
                .revenueSummary("미국 민간 AIP 고객 수 +83% 급증, 연간 잉여현금흐름 상향")
                .aiSummary("AIP(인공지능 플랫폼)의 미국 기업 고객 채택 속도가 가속화되며 연간 매출 및 영업이익 전망치를 연이어 상향했습니다. 방산 부문 수주도 견고합니다.")
                .sentiment("호재")
                .stockImpactPct(+2.45)
                .timestamp(now - 36000000)
                .detailEpsActual("$0.10")
                .detailEpsEstimate("$0.09")
                .detailEpsSurprise("+11.1% Beat")
                .detailRevActual("$7.26억 달러")
                .detailRevEstimate("$7.05억 달러")
                .detailRevGrowthYoY("+30.0% YoY")
                .detailGuidance("연간 매출 가이던스 $28.05억 ~ $28.09억 달러로 상향, 연간 조정 영업이익 10.5억 달러 상회 전망")
                .detailKeyDrivers("미국 민간 부문 AIP 부트캠프가 실계약으로 빠르게 전환되며 분기 흑자 행진 지속, 국방부 AI 수주 확대")
                .detailMarketReaction("S&P 500 지수 편입 후 패시브 자금 유입과 함께 주가 신고가 경신")
                .build());

        earningsEvents.add(EarningsEventDto.builder()
                .id("earn-intc-1")
                .ticker("INTC")
                .stockName("인텔 (Intel)")
                .date(todayDate)
                .time("08:30:00")
                .quarter("2026 Q3")
                .eventType("어닝 쇼크")
                .headline("Intel Reports Net Loss on Restructuring and Impairment Charges, Restructuring Underway")
                .epsSummary("조정 EPS -$0.46 (예상치 하회, 대규모 손상차손 반영)")
                .revenueSummary("파운드리 부문 영업손실 지속, 인력 15% 감축 계획 발표")
                .aiSummary("파운드리 투자 비용과 구조조정 비용으로 대규모 순손실을 기록했습니다. 18A 공정 전환까지 실적 불확실성이 지속될 것으로 평가되어 신중한 관망이 요구됩니다.")
                .sentiment("악재")
                .stockImpactPct(-3.80)
                .timestamp(now - 43200000)
                .detailEpsActual("-$0.46")
                .detailEpsEstimate("-$0.24")
                .detailEpsSurprise("-91.7% Miss (어닝 쇼크)")
                .detailRevActual("$132.8억 달러")
                .detailRevEstimate("$130.2억 달러")
                .detailRevGrowthYoY("-6.0% YoY")
                .detailGuidance("4분기 매출 가이던스 $133억 ~ $143억 달러 제시, 배당 지급 일시 중단 유지")
                .detailKeyDrivers("파운드리 사업부의 58억 달러 대규모 분기 영업손실 및 설비 자산 손상차손, PC용 CPU 시장 경쟁 심화")
                .detailMarketReaction("시간외 거래 4% 하락, 자구책으로 파운드리 분사 및 독립 법인화 추진 공시")
                .build());
    }
}
