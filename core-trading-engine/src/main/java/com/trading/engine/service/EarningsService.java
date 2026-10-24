package com.trading.engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.EarningsEventDto;
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
     * 미국 시총 상위 100대 기업 최근 핵심 실적 발표 및 가이던스 상향 시드 데이터
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
                .epsSummary("연간 주당순이익(EPS) 추정치 상향 조정")
                .revenueSummary("연간 매출 가이던스 $40억 달러(+40억 달러) 대폭 상향")
                .aiSummary("글로벌 AI 데이터센터 증설에 따른 수랭식 서버 랙 주문이 폭증하며 연간 매출을 40억 달러 전격 상향했습니다. 단순 실적 기대감을 넘어 실질적 현금 창출 팩트가 확인되었습니다.")
                .sentiment("호재")
                .stockImpactPct(+2.10)
                .timestamp(now - 3600000)
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
                .build());
    }
}
