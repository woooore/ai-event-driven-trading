FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

ENV TZ=Asia/Seoul

COPY app.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-Duser.timezone=Asia/Seoul", "-jar", "app.jar"]

