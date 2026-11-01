FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Seoul

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 30초 주기로 1차 키워드 필터 + Redis 유사도 캐싱 기반 24시간 실시간 모니터링 실행
CMD ["python", "-u", "analyzer.py", "--loop", "--interval", "30"]
