FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Seoul

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 60초 주기로 24시간 실시간 무한 모니터링 실행
CMD ["python", "-u", "analyzer.py", "--loop", "--interval", "60"]
