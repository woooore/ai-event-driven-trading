package com.trading.engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.AnalysisLogDto;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
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

    @Autowired(required = false)
    private RedissonClient redissonClient;

    private static final String LOG_STORAGE_FILE = "storage/analysis_logs.json";
    private static final String LOG_LEGACY_FILE = "analysis_logs.json";
    private static final String REDIS_KEY = "analysis:persisted_logs";
    private static final int MAX_LOGS = 5000; // 최대 5000개 영구 보관

    @PostConstruct
    public void init() {
        ensureStorageDir();
        loadFromStorage();
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
        if (dto.getTicker() != null) {
            dto.setStockName(PortfolioManager.resolveKoreanName(dto.getTicker(), dto.getStockName()));
        }

        // 중복 방지: 동일 ID 또는 동일 티커 + 날짜 + 시간 + 헤드라인 이미 존재 시 중복 추가 방지
        for (int i = 0; i < logs.size(); i++) {
            AnalysisLogDto exist = logs.get(i);
            if (exist.getId() != null && exist.getId().equals(dto.getId())) {
                logs.set(i, dto);
                saveToFile();
                return;
            }
            if (Objects.equals(exist.getTicker(), dto.getTicker())
                    && Objects.equals(exist.getDate(), dto.getDate())
                    && Objects.equals(exist.getTime(), dto.getTime())
                    && Objects.equals(exist.getHeadline(), dto.getHeadline())) {
                return;
            }
        }

        logs.add(0, dto);

        if (logs.size() > MAX_LOGS) {
            logs.remove(logs.size() - 1);
        }

        saveToFile();
    }

    public List<AnalysisLogDto> getLogsByDate(String dateStr) {
        return getLogsFiltered(dateStr, null, null);
    }

    /**
     * 날짜, 기업명/티커 키워드, 매매 판단(BUY/HOLD/SELL) 조건으로 AI 감지 로그 필터링 조회
     */
    public List<AnalysisLogDto> getLogsFiltered(String dateStr, String keyword, String action) {
        boolean allDates = (dateStr != null && "ALL".equalsIgnoreCase(dateStr.trim()));
        String targetDigits = allDates ? "" : normalizeDate(dateStr);
        String kw = (keyword != null) ? keyword.trim().toLowerCase() : "";
        String act = (action != null && !"ALL".equalsIgnoreCase(action.trim())) ? action.trim().toUpperCase() : "";

        List<AnalysisLogDto> filtered = new ArrayList<>();
        for (AnalysisLogDto l : logs) {
            // 1. 날짜 필터 (ALL이 아닐 경우 해당 날짜만, 단 기업 검색어가 있고 dateStr가 ALL이면 전체 기간 조회)
            if (!allDates) {
                if (!normalizeDate(l.getDate()).equals(targetDigits)) {
                    continue;
                }
            }

            // 2. 액션 필터 (BUY / HOLD / SELL)
            if (!act.isEmpty()) {
                if (l.getAction() == null || !l.getAction().equalsIgnoreCase(act)) {
                    continue;
                }
            }

            // 3. 기업명 / 티커 검색 필터
            if (!kw.isEmpty()) {
                String ticker = l.getTicker() != null ? l.getTicker().toLowerCase() : "";
                String stockName = l.getStockName() != null ? l.getStockName().toLowerCase() : "";
                String resolvedKor = PortfolioManager.resolveKoreanName(l.getTicker(), l.getStockName()).toLowerCase();
                if (!ticker.contains(kw) && !stockName.contains(kw) && !resolvedKor.contains(kw)) {
                    continue;
                }
            }

            filtered.add(l);
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
                .headline("미국 데이터센터 전력 수요 2030년까지 20% 급증 전망 및 AI 인프라 수혜 분석")
                .reason("현재 주가가 약보합세를 보이며, 단기 차익실현 물량을 소화하는 과정입니다. 기술 지표가 횡보 추세를 나타내고 있어 눌림목 지지 확인 후 매수하는 것이 적절합니다.")
                .rsi(52.4)
                .quantScore(62)
                .build());

        logs.add(AnalysisLogDto.builder()
                .id("al-seed-2")
                .timestamp(todayDate + " 20:45:10")
                .date(todayDate)
                .time("20:45:10")
                .ticker("HXSCL")
                .stockName("SK하이닉스 (SK Hynix ADR)")
                .action("BUY")
                .confidence(0.88)
                .newsImpactScore(86)
                .pricedInRisk("LOW")
                .positionSizePct(25.0)
                .orderPrice(142.50)
                .pctChange(+2.40)
                .headline("SK하이닉스, 3분기 영업이익 7.03조 사상 최대 실적 및 대규모 주주환원 밸류업 발표")
                .reason("HBM3E 12단 독점 공급과 역대 최대 분기 영업이익 달성, 잉여현금흐름 50% 주주환원 발표로 강력한 펀더멘털 모멘텀이 발생했습니다. 퀀트 점수 86점으로 적극 매수 적격입니다.")
                .rsi(56.2)
                .quantScore(86)
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
                .positionSizePct(25.0)
                .orderPrice(48.50)
                .pctChange(+2.10)
                .headline("슈퍼마이크로, AI 수랭식 서버 수요 폭증으로 연간 매출 가이던스 40억 달러 전격 상향")
                .reason("슈퍼마이크로가 40억 달러의 대규모 연간 매출 상향 조정을 발표하며 실질적 펀더멘털 개선이 확인되었습니다. 당일 변동률(+2.1%)이 과열권이 아니며 RSI(54.2)도 안정적이므로 집중 매수합니다.")
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
                .positionSizePct(25.0)
                .orderPrice(43.20)
                .pctChange(+1.80)
                .headline("팔란티어, 미 국방부 AI 수주 확장 및 민간 기업 AIP 계약 +83% 급증")
                .reason("국방 AI 솔루션 수주 계약 확장으로 실질적인 매출 파이프라인이 견고해졌습니다. 기술 지표와 거래량 모멘텀이 뒷받침되어 매수를 집행합니다.")
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
                .action("BUY")
                .confidence(0.84)
                .newsImpactScore(80)
                .pricedInRisk("LOW")
                .positionSizePct(25.0)
                .orderPrice(218.40)
                .pctChange(-0.80)
                .headline("테슬라 & 아폴로, xAI 대규모 GPU 데이터센터 구축 금융 리스 계약 체결 및 3분기 마진 서프라이즈")
                .reason("GPU 인프라 대규모 금융 조달 체결 및 차량 원가 절감 호재에도 불구하고 단기 눌림목(-0.8%) 구간에 위치하여 저가 매수 기회로 판단됩니다.")
                .rsi(49.8)
                .quantScore(81)
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

    private void ensureStorageDir() {
        try {
            Path dir = Path.of("storage");
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
        } catch (Exception ignored) {}
    }

    private void loadFromStorage() {
        List<AnalysisLogDto> rawLoaded = null;

        // 1. Redis에서 우선 복원 시도
        if (redissonClient != null) {
            try {
                RBucket<String> bucket = redissonClient.getBucket(REDIS_KEY);
                if (bucket.isExists()) {
                    String json = bucket.get();
                    if (json != null && !json.isBlank()) {
                        rawLoaded = objectMapper.readValue(json, new TypeReference<>() {});
                        log.info(">> [AI 감지 로그] Redis에서 분석 로그 {}건 발견", rawLoaded != null ? rawLoaded.size() : 0);
                    }
                }
            } catch (Exception e) {
                log.warn("Redis AI 감지 로그 복원 오류: {}", e.getMessage());
            }
        }

        // 2. 로컬 storage/analysis_logs.json 또는 기존 analysis_logs.json 확인 (더 많은 쪽 채택)
        File storageFile = new File(LOG_STORAGE_FILE);
        File legacyFile = new File(LOG_LEGACY_FILE);
        File target = (storageFile.exists() && storageFile.isFile() && storageFile.length() > 0) ? storageFile :
                      ((legacyFile.exists() && legacyFile.isFile() && legacyFile.length() > 0) ? legacyFile : null);

        if (target != null) {
            try {
                List<AnalysisLogDto> fileLoaded = objectMapper.readValue(target, new TypeReference<>() {});
                if (rawLoaded == null || (fileLoaded != null && fileLoaded.size() > rawLoaded.size())) {
                    rawLoaded = fileLoaded;
                }
            } catch (Exception e) {
                log.error("AI 분석 로그 파일 로드 실패: {}", e.getMessage());
            }
        }

        if (rawLoaded != null && !rawLoaded.isEmpty()) {
            logs.clear();
            Set<String> seenKeys = new HashSet<>();
            for (AnalysisLogDto l : rawLoaded) {
                if (l == null) continue;
                String contentKey = l.getTicker() + "|" + l.getDate() + "|" + l.getTime() + "|" + l.getHeadline();
                String idKey = l.getId() != null ? l.getId() : "";
                if (seenKeys.contains(contentKey) || (!idKey.isEmpty() && seenKeys.contains(idKey))) {
                    continue;
                }
                seenKeys.add(contentKey);
                if (!idKey.isEmpty()) seenKeys.add(idKey);
                if (l.getTicker() != null) {
                    l.setStockName(PortfolioManager.resolveKoreanName(l.getTicker(), l.getStockName()));
                }
                logs.add(l);
            }
            log.info(">> 💾 [AI 감지 로그 영구 보존 복원 완료] 총 {}건 로드 완료", logs.size());
            saveToFile();
        }
    }

    private void saveToFile() {
        try {
            ensureStorageDir();
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(logs);
            Files.writeString(Path.of(LOG_STORAGE_FILE), json);
            if (redissonClient != null) {
                try {
                    redissonClient.getBucket(REDIS_KEY).set(json);
                } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            log.error("AI 분석 로그 파일 저장 실패: {}", e.getMessage());
        }
    }
}
