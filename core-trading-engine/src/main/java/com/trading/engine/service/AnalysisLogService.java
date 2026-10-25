package com.trading.engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.AnalysisLogDto;
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
public class AnalysisLogService {

    private final ObjectMapper objectMapper;
    private final List<AnalysisLogDto> logs = new CopyOnWriteArrayList<>();
    private static final String LOG_FILE = "analysis_logs.json";
    private static final int MAX_LOGS = 5000; // 최대 5000개 보관

    @PostConstruct
    public void init() {
        loadFromFile();
        if (logs.isEmpty()) {
            initSeedLogs();
            saveToFile();
        }
    }

    public synchronized void addLog(AnalysisLogDto dto) {
        if (dto == null) return;
        if (dto.getId() == null) {
            dto.setId("al-" + System.currentTimeMillis() + "-" + (int)(Math.random() * 1000));
        }
        if (dto.getDate() == null) {
            dto.setDate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }

        // 중복 방지: 동일 ID 또는 동일 티커 + 날짜 + 시간 + 헤드라인 이미 존재 시 중복 추가 방지
        for (int i = 0; i < logs.size(); i++) {
            AnalysisLogDto exist = logs.get(i);
            if (exist.getId() != null && exist.getId().equals(dto.getId())) {
                logs.set(i, dto); // 기존 동일 ID 항목 업데이트
                saveToFile();
                return;
            }
            if (Objects.equals(exist.getTicker(), dto.getTicker())
                    && Objects.equals(exist.getDate(), dto.getDate())
                    && Objects.equals(exist.getTime(), dto.getTime())
                    && Objects.equals(exist.getHeadline(), dto.getHeadline())) {
                return; // 이미 등록된 동일 분석 로그이므로 무시
            }
        }

        logs.add(0, dto); // 최신순 앞에 추가

        if (logs.size() > MAX_LOGS) {
            logs.remove(logs.size() - 1);
        }

        saveToFile();
    }

    public List<AnalysisLogDto> getLogsByDate(String dateStr) {
        String targetDigits = normalizeDate(dateStr);

        List<AnalysisLogDto> filtered = new ArrayList<>();
        for (AnalysisLogDto l : logs) {
            if (normalizeDate(l.getDate()).equals(targetDigits)) {
                filtered.add(l);
            }
        }
        return filtered;
    }

    private String normalizeDate(String d) {
        if (d == null || d.isBlank()) {
            return LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
        return d.replaceAll("[^0-9]", "");
    }

    private void initSeedLogs() {
        String todayDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String todayTime = "21:08:49";

        logs.add(AnalysisLogDto.builder()
                .id("al-seed-1")
                .timestamp(todayDate + " 21:08:49")
                .date(todayDate)
                .time("21:08:49")
                .ticker("NVDA")
                .stockName("엔비디아 (NVIDIA)")
                .action("HOLD")
                .confidence(0.80)
                .newsImpactScore(55)
                .pricedInRisk("LOW")
                .positionSizePct(0.0)
                .orderPrice(210.96)
                .pctChange(-0.59)
                .headline("U.S. Power Demand Is Set to Jump 20% by 2030. Here's What It Means for Energy Sector")
                .reason("현재 주가가 약세를 보이며, 뉴스 피드에서도 엔비디아 관련 직접적인 호재가 부족한 상태입니다. 기술 지표 또한 횡보 추세를 나타내고 있어 추가 매수보다는 관망하는 것이 적절합니다.")
                .rsi(52.4)
                .quantScore(62)
                .build());

        logs.add(AnalysisLogDto.builder()
                .id("al-seed-2")
                .timestamp(todayDate + " 20:45:10")
                .date(todayDate)
                .time("20:45:10")
                .ticker("UNH")
                .stockName("유나이티드헬스 (UnitedHealth)")
                .action("HOLD")
                .confidence(0.80)
                .newsImpactScore(70)
                .pricedInRisk("MEDIUM")
                .positionSizePct(0.0)
                .orderPrice(419.89)
                .pctChange(-1.20)
                .headline("What Will Decide UnitedHealth Group Stock Over The Next Six Months?")
                .reason("최근 3분기 실적에서 조정 EPS가 61% 증가하며 시장 기대치를 초과했으나, RSI가 70.7로 단기 과열 구간에 접근해 있어 차익실현 리스크 방어를 위해 신중하게 관망합니다.")
                .rsi(70.7)
                .quantScore(68)
                .build());

        logs.add(AnalysisLogDto.builder()
                .id("al-seed-3")
                .timestamp(todayDate + " 20:12:35")
                .date(todayDate)
                .time("20:12:35")
                .ticker("SMCI")
                .stockName("슈퍼마이크로 (Supermicro)")
                .action("BUY")
                .confidence(0.85)
                .newsImpactScore(82)
                .pricedInRisk("LOW")
                .positionSizePct(7.5)
                .orderPrice(48.50)
                .pctChange(+2.10)
                .headline("Supermicro Raises Full-Year Revenue Guidance by $4 Billion on Massive AI Server Demand")
                .reason("슈퍼마이크로가 40억 달러의 대규모 연간 매출 상향 조정을 발표하며 실질적 펀더멘털 개선이 확인되었습니다. 당일 변동률(+2.1%)이 과열권이 아니며 RSI(54.2)도 안정적이므로 적극 분할 매수합니다.")
                .rsi(54.2)
                .quantScore(84)
                .build());

        logs.add(AnalysisLogDto.builder()
                .id("al-seed-4")
                .timestamp(todayDate + " 19:30:22")
                .date(todayDate)
                .time("19:30:22")
                .ticker("PLTR")
                .stockName("팔란티어 (Palantir)")
                .action("BUY")
                .confidence(0.82)
                .newsImpactScore(78)
                .pricedInRisk("LOW")
                .positionSizePct(6.0)
                .orderPrice(43.20)
                .pctChange(+1.80)
                .headline("Palantir Secures Multi-Million Dollar Defense AI Expansion Contract")
                .reason("국방 AI 솔루션 수주 계약 확장으로 실질적인 매출 파이프라인이 견고해졌습니다. 기술 지표와 거래량 모멘텀이 뒷받침되어 권장 비중 6% 매수를 집행합니다.")
                .rsi(61.5)
                .quantScore(79)
                .build());

        logs.add(AnalysisLogDto.builder()
                .id("al-seed-5")
                .timestamp(todayDate + " 18:40:15")
                .date(todayDate)
                .time("18:40:15")
                .ticker("TSLA")
                .stockName("테슬라 (Tesla)")
                .action("HOLD")
                .confidence(0.75)
                .newsImpactScore(50)
                .pricedInRisk("MEDIUM")
                .positionSizePct(0.0)
                .orderPrice(218.40)
                .pctChange(-0.80)
                .headline("Tesla Robotaxi Roadmap Faces Regulatory Scrutiny in Key States")
                .reason("로보택시 로드맵에 대한 규제 검토 뉴스가 보도되었으나 즉각적인 실적 영향은 제한적입니다. 지표가 중립권에 위치하므로 관망을 유지합니다.")
                .rsi(49.8)
                .quantScore(56)
                .build());
    }

    public List<String> getAvailableDates() {
        Set<String> dates = new TreeSet<>(Comparator.reverseOrder());
        for (AnalysisLogDto l : logs) {
            if (l.getDate() != null) {
                dates.add(l.getDate());
            }
        }
        if (dates.isEmpty()) {
            dates.add(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }
        return new ArrayList<>(dates);
    }

    private void loadFromFile() {
        File file = new File(LOG_FILE);
        if (file.exists() && file.length() > 0) {
            try {
                List<AnalysisLogDto> loaded = objectMapper.readValue(file, new TypeReference<>() {});
                logs.clear();
                Set<String> seenKeys = new HashSet<>();
                for (AnalysisLogDto l : loaded) {
                    // ID가 같거나 (티커 + 날짜 + 시간 + 헤드라인)이 동일한 중복 로그 필터링
                    String contentKey = l.getTicker() + "|" + l.getDate() + "|" + l.getTime() + "|" + l.getHeadline();
                    String idKey = l.getId() != null ? l.getId() : "";
                    if (seenKeys.contains(contentKey) || (!idKey.isEmpty() && seenKeys.contains(idKey))) {
                        continue;
                    }
                    seenKeys.add(contentKey);
                    if (!idKey.isEmpty()) seenKeys.add(idKey);
                    logs.add(l);
                }
                log.info(">> [AI 감지 로그] 저장된 분석 로그 {}건 로드 완료 (중복 제거 후)", logs.size());
                saveToFile();
            } catch (Exception e) {
                log.error("AI 분석 로그 파일 로드 실패: {}", e.getMessage());
            }
        }
    }

    private void saveToFile() {
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(LOG_FILE), logs);
        } catch (Exception e) {
            log.error("AI 분석 로그 파일 저장 실패: {}", e.getMessage());
        }
    }
}
