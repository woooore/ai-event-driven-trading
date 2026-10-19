\# AI 기반 자동 매매 시스템 (Event-Driven Trading System)



> \*\*LLM 뉴스 분석 시그널과 Kafka 비동기 파이프라인을 활용한 안정적인 주식 매매 백엔드 시스템\*\*



\## 프로젝트 목적

단일 언어로 구성된 단순 자동 매매 스크립트를 넘어, \*\*대용량 이벤트 처리와 시스템 신뢰성(동시성 제어, 장애 격리)을 보장하는 백엔드 아키텍처 설계\*\*에 집중한 프로젝트입니다.

AI(Python) 워커와 주문 실행(Java) 코어 엔진을 물리적으로 분리하여 마이크로서비스(MSA) 관점의 설계 역량을 증명하고자 합니다.



\## 기술 스택

\- \*\*Core Engine\*\*: Java 17, Spring Boot 3.x, Spring Data JPA

\- \*\*AI Worker\*\*: Python 3.10, OpenAI API (GPT-4o-mini), Pydantic

\- \*\*Infrastructure \& Middleware\*\*: Kafka, Redis (Redisson 분산 락), Docker

\- \*\*External API\*\*: KIS(한국투자증권) Developers API, DART 오픈 API



\## 아키텍처

(추후 시스템 구성도 이미지 첨부 예정)

1\. \*\*AI Signal Worker (Python)\*\*: 뉴스/공시 크롤링 -> LLM 감성 분석 -> Kafka 매수 시그널 발행

2\. \*\*Message Broker (Kafka)\*\*: 비동기 이벤트 전달 및 버퍼링 (장애 격리)

3\. \*\*Core Trading Engine (Java/Spring)\*\*: 시그널 컨슘 -> 분산 락(중복 제어) -> 잔고 검증 -> 주문 체결 처리

