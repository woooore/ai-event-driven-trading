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
    }

    public synchronized void addLog(AnalysisLogDto dto) {
        if (dto == null) return;
        if (dto.getId() == null) {
            dto.setId("al-" + System.currentTimeMillis() + "-" + (int)(Math.random() * 1000));
        }
        if (dto.getDate() == null) {
            dto.setDate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }

        logs.add(0, dto); // 최신순 앞에 추가

        if (logs.size() > MAX_LOGS) {
            logs.remove(logs.size() - 1);
        }

        saveToFile();
    }

    public List<AnalysisLogDto> getLogsByDate(String dateStr) {
        String targetDate = (dateStr == null || dateStr.isBlank()) 
                ? LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                : dateStr.trim();

        List<AnalysisLogDto> filtered = new ArrayList<>();
        for (AnalysisLogDto l : logs) {
            if (targetDate.equals(l.getDate())) {
                filtered.add(l);
            }
        }
        return filtered;
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
                logs.addAll(loaded);
                log.info(">> [AI 감지 로그] 저장된 분석 로그 {}건 로드 완료", logs.size());
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
