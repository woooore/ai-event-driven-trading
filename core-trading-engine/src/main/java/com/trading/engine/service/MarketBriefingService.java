package com.trading.engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.dto.MarketBriefingDto;
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
public class MarketBriefingService {

    private final ObjectMapper objectMapper;
    private final List<MarketBriefingDto> briefings = new CopyOnWriteArrayList<>();
    private static final String BRIEFING_FILE = "market_briefings.json";

    @PostConstruct
    public void init() {
        loadFromFile();
        if (briefings.isEmpty()) {
            initSeedBriefings();
            saveToFile();
        }
    }

    /**
     * 특정 날짜의 1시간 단위 AI 브리핑 목록 조회 (최신 시간순 정렬)
     * date 파라미터 예: "2026-10-24" or "2026. 10. 24."
     */
    public List<MarketBriefingDto> getBriefingsByDate(String dateStr) {
        String normalizedTarget = normalizeDate(dateStr);
        List<MarketBriefingDto> result = new ArrayList<>();

        for (MarketBriefingDto b : briefings) {
            if (normalizeDate(b.getDate()).equals(normalizedTarget)) {
                result.add(b);
            }
        }

        // 시간 역순 정렬 (06:00 -> 05:00 -> 04:00 ...)
        result.sort((a, b) -> b.getTime().compareTo(a.getTime()));
        return result;
    }

    /**
     * 브리핑 데이터가 존재하는 날짜 목록 반환 (최신순)
     */
    public List<String> getAvailableDates() {
        Set<String> dateSet = new TreeSet<>(Comparator.reverseOrder());
        for (MarketBriefingDto b : briefings) {
            dateSet.add(normalizeDateToDisplay(b.getDate()));
        }
        if (dateSet.isEmpty()) {
            dateSet.add(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy. MM. dd.")));
        }
        return new ArrayList<>(dateSet);
    }

    /**
     * 새로운 1시간 단위 AI 브리핑 추가
     */
    public synchronized void addBriefing(MarketBriefingDto dto) {
        if (dto == null) return;
        if (dto.getId() == null) {
            dto.setId("briefing-" + System.currentTimeMillis());
        }
        if (dto.getTimestamp() == null) {
            dto.setTimestamp(System.currentTimeMillis());
        }

        // 동일 날짜/시간 브리핑이 이미 있으면 업데이트, 없으면 추가
        boolean updated = false;
        for (int i = 0; i < briefings.size(); i++) {
            MarketBriefingDto exist = briefings.get(i);
            if (normalizeDate(exist.getDate()).equals(normalizeDate(dto.getDate()))
                    && exist.getTime().equals(dto.getTime())) {
                briefings.set(i, dto);
                updated = true;
                break;
            }
        }
        if (!updated) {
            briefings.add(0, dto);
        }

        saveToFile();
        log.info(">> ✨ [AI 1시간 브리핑 등록] {} {} - {}", dto.getDate(), dto.getTime(), dto.getTitle());
    }

    private String normalizeDate(String d) {
        if (d == null) return LocalDate.now().toString();
        return d.replaceAll("[^0-9]", "");
    }

    private String normalizeDateToDisplay(String d) {
        String num = normalizeDate(d);
        if (num.length() == 8) {
            return num.substring(0, 4) + ". " + num.substring(4, 6) + ". " + num.substring(6, 8) + ".";
        }
        return d;
    }

    private void loadFromFile() {
        File file = new File(BRIEFING_FILE);
        if (file.exists() && file.length() > 0) {
            try {
                List<MarketBriefingDto> loaded = objectMapper.readValue(file, new TypeReference<>() {});
                briefings.clear();
                briefings.addAll(loaded);
                log.info(">> [AI 브리핑] 저장된 브리핑 {}건 로드 완료", briefings.size());
            } catch (Exception e) {
                log.error("AI 브리핑 파일 로드 실패: {}", e.getMessage());
            }
        }
    }

    private void saveToFile() {
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(BRIEFING_FILE), briefings);
        } catch (Exception e) {
            log.error("AI 브리핑 파일 저장 실패: {}", e.getMessage());
        }
    }

    /**
     * 첨부 이미지에 나온 100% 동일한 고품질 1시간 단위 시드 데이터 초기화
     */
    private void initSeedBriefings() {
        String todayStr = "2026. 10. 24.";
        long now = System.currentTimeMillis();

        briefings.add(MarketBriefingDto.builder()
                .id("b-0600")
                .date(todayStr)
                .time("06:00")
                .title("금리 상승에 다우 약세, 반도체만 버틴 혼조 마감")
                .category("혼조")
                .bullets(List.of(
                        "미국장은 장 막판까지 무거웠고, 다우가 크게 밀리면서 전체 분위기를 눌렀어요.",
                        "국채금리가 다시 올라가고 달러도 강해지면서 금융주와 소비 관련 대형주에 부담이 커졌어요.",
                        "그런데 반도체 지수는 올랐고 TSMC와 마이크론이 강세를 보이면서 기술주 안에서도 온도차가 뚜렷했어요."
                ))
                .timestamp(now - 3600000)
                .build());

        briefings.add(MarketBriefingDto.builder()
                .id("b-0500")
                .date(todayStr)
                .time("05:00")
                .title("미 국채금리 5% 재진입에 다우 1.2% 하락, 반도체만 선방 마감")
                .category("악재")
                .bullets(List.of(
                        "금리가 다시 고개를 들자 다우가 크게 밀리고 시장 분위기가 빠르게 식었어요.",
                        "미 국채 10년물이 5%대로 올라서면서, 비싸게 평가받던 종목부터 매도가 나왔어요.",
                        "그런데 반도체는 SOX가 올랐고, TSMC가 강세로 마감했어요."
                ))
                .timestamp(now - 7200000)
                .build());

        briefings.add(MarketBriefingDto.builder()
                .id("b-0400")
                .date(todayStr)
                .time("04:00")
                .title("다우 1%대 하락 속 SOX 상승, 금리·유가 부담에 경기민감주 눌림")
                .category("혼조")
                .bullets(List.of(
                        "지난 1시간 동안 다우가 낙폭을 키우는 사이 SOX는 상승 흐름을 이어가고 있어요.",
                        "국채금리와 WTI가 함께 오르면서, 경기 부담이 커진 쪽부터 매도세가 붙고 있어요.",
                        "S&P500은 하락인데 나스닥100은 보합권이라, 지수 안에서도 체감 온도가 갈리고 있어요."
                ))
                .timestamp(now - 10800000)
                .build());

        briefings.add(MarketBriefingDto.builder()
                .id("b-0300")
                .date(todayStr)
                .time("03:00")
                .title("금리·유가 상승에 다우 약세, 반도체는 버티는 장세")
                .category("혼조")
                .bullets(List.of(
                        "지난 1시간에도 다우 약세가 이어지는 가운데 나스닥은 보합권을 지키고 있어요.",
                        "국채금리와 유가가 함께 오르면서 경기민감주와 소비주 쪽 부담이 더 커지고 있어요.",
                        "그런데 반도체주는 상대적으로 강해 SOX가 오르고, 대형 기술주 안에서도 온도가 나고 있어요."
                ))
                .timestamp(now - 14400000)
                .build());

        briefings.add(MarketBriefingDto.builder()
                .id("b-0200")
                .date(todayStr)
                .time("02:00")
                .title("유가 3%대 상승과 금리 동반 상승에 다우 약세, 나스닥은 반도체로 버팀")
                .category("혼조")
                .bullets(List.of(
                        "WTI가 3% 넘게 오르면서 경기민감주 중심의 다우 약세가 두드러져요.",
                        "2년물과 10년물 금리가 함께 올라, 금융·소비 쪽엔 부담이 커지고 있어요.",
                        "하지만 엔비디아와 TSMC 등 AI 반도체 밸류체인은 견고한 지지력을 보이고 있어요."
                ))
                .timestamp(now - 18000000)
                .build());
    }
}
