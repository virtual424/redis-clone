# ---- Build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Download dependencies first so they are cached between builds
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q -DskipTests package

# ---- Run ----
FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /app/target/redis-clone-*.jar app.jar

# Keep the AOF on a volume so data survives container restarts
VOLUME /data

ENV REDIS_HOST=0.0.0.0 \
    REDIS_PORT=6369 \
    REDIS_AOFFILE=/data/appendonly.aof

EXPOSE 6369

ENTRYPOINT ["java", "-jar", "app.jar"]
