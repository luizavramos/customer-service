FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/customer-service.jar app.jar
RUN mkdir -p /data
EXPOSE 8083
ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=60", "-jar", "/app/app.jar"]
