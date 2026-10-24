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
        if (d == null || d.isBlank()) {
            return LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
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
     * 한국 표준시(KST) 기준 02:00부터 21:00까지 하루 24시간 전체 1시간 단위 AI 브리핑 데이터 초기화
     */
    private void initSeedBriefings() {
        String todayStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy. MM. dd."));
        long now = System.currentTimeMillis();

        // 21:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-2100").date(todayStr).time("21:00")
                .title("미 선물 보합권 유지 속 빅테크 중심 선별 매수세 유입")
                .category("호재")
                .bullets(List.of(
                        "미 야간 선물 시장은 소폭 반등을 시도하며 기술주 중심으로 저가 매수세가 유입되고 있어요.",
                        "엔비디아와 브로드컴 등 AI 인프라 핵심 수혜주에 대한 월가 투자은행들의 목표가 상향 리포트가 전해졌어요.",
                        "다만 달러 인덱스가 여전히 104선을 유지하고 있어 외환 시장 변동성 경계감은 남아있어요."
                ))
                .timestamp(now).build());

        // 20:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-2000").date(todayStr).time("20:00")
                .title("유럽 증시 기술주 반등 마감, 미 국채금리 4.8%대 안정세")
                .category("호재")
                .bullets(List.of(
                        "유럽 주요 증시는 반도체 장비주와 에너지 섹터 주도로 상승 마감했어요.",
                        "급등세를 타던 미 국채 10년물 금리가 4.8%대로 소폭 내려앉으며 기술주 밸류에이션 부담을 덜어줬어요.",
                        "슈퍼마이크로의 40억 달러 매출 가이던스 상향 소식 이후 AI 서버 부품주들의 시간외 거래가 활발해졌어요."
                ))
                .timestamp(now - 3600000).build());

        // 19:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1900").date(todayStr).time("19:00")
                .title("빅테크 AI 설비투자 지속 기대에 기술주 프리마켓 강세")
                .category("호재")
                .bullets(List.of(
                        "마이크로소프트와 메타의 차세대 AI 데이터센터 증설 계획이 보도되며 투자심리가 회복세를 보이고 있어요.",
                        "미국 나스닥 지수 선물은 0.4% 상승세를 기록하며 오늘 밤 정규장 반등을 모색하고 있어요.",
                        "국제 유가는 중동 지정학적 긴장 완화 조짐에 소폭 하락하며 숨고르기에 들어갔어요."
                ))
                .timestamp(now - 7200000).build());

        // 18:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1800").date(todayStr).time("18:00")
                .title("아시아 증시 반도체 강세 마감 속 미 선물 상승 전환")
                .category("호재")
                .bullets(List.of(
                        "대만과 일본 증시가 TSMC 실적 호조 훈풍을 이어받아 상승으로 거래를 마쳤어요.",
                        "나스닥100 선물 지수가 플러스권으로 돌아서며 오늘 밤 뉴욕 정규장에 대한 기대감을 키우고 있어요.",
                        "반도체 공급망 전반에 걸쳐 하반기 수요 회복에 대한 긍정적인 전망이 이어지고 있어요."
                ))
                .timestamp(now - 10800000).build());

        // 17:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1700").date(todayStr).time("17:00")
                .title("글로벌 헤지펀드 실적 서프라이즈 우량주 비중 확대")
                .category("혼조")
                .bullets(List.of(
                        "주요 글로벌 기관 투자자들이 호실적을 발표한 헬스케어 및 AI 테크 우량주에 매수세를 집중하고 있어요.",
                        "유나이티드헬스(UNH)의 분기 조정 EPS 61% 증가 서프라이즈로 다우 지수 하방 경직성이 확보되고 있어요.",
                        "금리 민감주들은 여전히 신중한 흐름이지만, 펀더멘털이 확실한 대형주는 차별화 장세를 나타내고 있어요."
                ))
                .timestamp(now - 14400000).build());

        // 16:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1600").date(todayStr).time("16:00")
                .title("미 국채 금리 급등세 진정, 기술주 반등 모멘텀 탐색")
                .category("혼조")
                .bullets(List.of(
                        "아시아 시간대 미 국채 10년물 금리가 상승세를 멈추고 횡보세를 이어가고 있어요.",
                        "원/달러 환율도 1,350원 초반대에서 안정을 찾으며 외국인 수급 여건이 개선되고 있어요.",
                        "빅테크 및 반도체 섹터는 전일 뉴욕장 마감 이후 낙폭 과대 인식에 따른 반등 에너지가 축적되고 있어요."
                ))
                .timestamp(now - 18000000).build());

        // 15:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1500").date(todayStr).time("15:00")
                .title("국내장 마감: 반도체 투톱 외인 매수에 코스피 강보합")
                .category("호재")
                .bullets(List.of(
                        "외국인 투자자들의 전기전자 업종 순매수에 힘입어 코스피가 강보합권에서 거래를 마쳤어요.",
                        "미국 반도체 SOX 지수 강세가 아시아 반도체 밸류체인 전반에 긍정적인 파급효과를 줬어요.",
                        "환율과 유가 변동성에도 불구하고 AI 하드웨어 밸류체인의 견조한 펀더멘털이 확인되고 있어요."
                ))
                .timestamp(now - 21600000).build());

        // 14:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1400").date(todayStr).time("14:00")
                .title("TSMC 및 아시아 파운드리 가동률 상승 전망에 온기 확산")
                .category("호재")
                .bullets(List.of(
                        "글로벌 빅테크의 차세대 AI 칩 주문 폭주로 파운드리 가동률이 100%에 근접하고 있다는 업계 보고서가 나왔어요.",
                        "엔비디아의 블랙웰 칩 출하 본격화 기대감이 관련 반도체 소부장 종목들에 온기를 불어넣고 있어요.",
                        "달러화 강세 압력은 점차 둔화되는 양상이에요."
                ))
                .timestamp(now - 25200000).build());

        // 13:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1300").date(todayStr).time("13:00")
                .title("중국 경기 부양책 기대감에 중화권 증시 반등, 미 선물 안정")
                .category("혼조")
                .bullets(List.of(
                        "중국 인민은행의 추가 유동성 공급 시사로 홍콩 및 상하이 지수가 반등세를 탔어요.",
                        "아시아 증시 전반에 위험자산 선호 심리가 다소 회복되며 미 선물도 낙폭을 만회했어요.",
                        "다만 고금리 장기화에 대한 경계감으로 경기 방어주와 성장주 간의 양극화는 계속되고 있어요."
                ))
                .timestamp(now - 28800000).build());

        // 12:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1200").date(todayStr).time("12:00")
                .title("미 국채 입찰 앞두고 채권 금리 관망세, 달러 강세 주춤")
                .category("혼조")
                .bullets(List.of(
                        "미 재무부 국채 입찰을 앞두고 채권 시장 참여자들의 관망세가 짙어지고 있어요.",
                        "달러 강세가 주춤해지면서 신흥국 통화 및 자산 가격이 진정세를 보였어요.",
                        "기술주 중심의 나스닥 선물은 보합권에서 추가 방향성을 모색 중이에요."
                ))
                .timestamp(now - 32400000).build());

        // 11:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1100").date(todayStr).time("11:00")
                .title("AI 소프트웨어/클라우드 기업 수주 훈풍에 투자심리 개선")
                .category("호재")
                .bullets(List.of(
                        "팔란티어와 오라클 등 주요 클라우드 및 엔터프라이즈 AI 기업들의 대규모 정부/민간 수주 소식이 부각됐어요.",
                        "단순 테마를 넘어 실질적인 AI 매출 가시성이 높은 기업들로 시장의 관심이 이동하고 있어요.",
                        "금리 부담 속에서도 펀더멘털 우량주의 프리미엄은 더욱 부각되는 분위기예요."
                ))
                .timestamp(now - 36000000).build());

        // 10:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-1000").date(todayStr).time("10:00")
                .title("뉴욕장 마감 분석: 반도체 홀로 선방, 금리 부담에 다우 약세")
                .category("혼조")
                .bullets(List.of(
                        "뉴욕증시는 국채금리 5% 재진입 부담으로 다우가 밀렸으나 반도체(SOX)는 상승하며 뚜렷한 차별화를 보였어요.",
                        "아시아 장 개장 직후 글로벌 투자자들은 금리 상승의 악영향이 덜한 현금 부자 빅테크로 피신하고 있어요.",
                        "AI 반도체 밸류체인은 장기 성장성에 대한 신뢰로 탄탄한 하방 지지력을 확인했어요."
                ))
                .timestamp(now - 39600000).build());

        // 09:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-0900").date(todayStr).time("09:00")
                .title("한국 및 아시아 장 개장, 美 혼조 마감 영향에 제한적 등락")
                .category("혼조")
                .bullets(List.of(
                        "미국 증시의 엇갈린 행보를 이어받아 아시아 주요 증시도 혼조세로 출발했어요.",
                        "경기민감 금융/소비주는 조정을 받는 반면, AI 반도체 관련주에는 매수세가 선별 유입되고 있어요.",
                        "미 국채 금리의 추가 상승 여부가 오늘 아시아 장세의 주요 변수로 작용하고 있어요."
                ))
                .timestamp(now - 43200000).build());

        // 06:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-0600").date(todayStr).time("06:00")
                .title("금리 상승에 다우 약세, 반도체만 버틴 혼조 마감")
                .category("혼조")
                .bullets(List.of(
                        "미국장은 장 막판까지 무거웠고, 다우가 크게 밀리면서 전체 분위기를 눌렀어요.",
                        "국채금리가 다시 올라가고 달러도 강해지면서 금융주와 소비 관련 대형주에 부담이 커졌어요.",
                        "그런데 반도체 지수는 올랐고 TSMC와 마이크론이 강세를 보이면서 기술주 안에서도 온도차가 뚜렷했어요."
                ))
                .timestamp(now - 54000000).build());

        // 05:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-0500").date(todayStr).time("05:00")
                .title("미 국채금리 5% 재진입에 다우 1.2% 하락, 반도체만 선방 마감")
                .category("악재")
                .bullets(List.of(
                        "금리가 다시 고개를 들자 다우가 크게 밀리고 시장 분위기가 빠르게 식었어요.",
                        "미 국채 10년물이 5%대로 올라서면서, 비싸게 평가받던 종목부터 매도가 나왔어요.",
                        "그런데 반도체는 SOX가 올랐고, TSMC가 강세로 마감했어요."
                ))
                .timestamp(now - 57600000).build());

        // 04:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-0400").date(todayStr).time("04:00")
                .title("다우 1%대 하락 속 SOX 상승, 금리·유가 부담에 경기민감주 눌림")
                .category("혼조")
                .bullets(List.of(
                        "지난 1시간 동안 다우가 낙폭을 키우는 사이 SOX는 상승 흐름을 이어가고 있어요.",
                        "국채금리와 WTI가 함께 오르면서, 경기 부담이 커진 쪽부터 매도세가 붙고 있어요.",
                        "S&P500은 하락인데 나스닥100은 보합권이라, 지수 안에서도 체감 온도가 갈리고 있어요."
                ))
                .timestamp(now - 61200000).build());

        // 03:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-0300").date(todayStr).time("03:00")
                .title("금리·유가 상승에 다우 약세, 반도체는 버티는 장세")
                .category("혼조")
                .bullets(List.of(
                        "지난 1시간에도 다우 약세가 이어지는 가운데 나스닥은 보합권을 지키고 있어요.",
                        "국채금리와 유가가 함께 오르면서 경기민감주와 소비주 쪽 부담이 더 커지고 있어요.",
                        "그런데 반도체주는 상대적으로 강해 SOX가 오르고, 대형 기술주 안에서도 온도가 나고 있어요."
                ))
                .timestamp(now - 64800000).build());

        // 02:00 KST
        briefings.add(MarketBriefingDto.builder()
                .id("b-0200").date(todayStr).time("02:00")
                .title("유가 3%대 상승과 금리 동반 상승에 다우 약세, 나스닥은 반도체로 버팀")
                .category("혼조")
                .bullets(List.of(
                        "WTI가 3% 넘게 오르면서 경기민감주 중심의 다우 약세가 두드러져요.",
                        "2년물과 10년물 금리가 함께 올라, 금융·소비 쪽엔 부담이 커지고 있어요.",
                        "하지만 엔비디아와 TSMC 등 AI 반도체 밸류체인은 견고한 지지력을 보이고 있어요."
                ))
                .timestamp(now - 68400000).build());
    }
}
