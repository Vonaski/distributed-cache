FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml ./
COPY common/pom.xml ./common/
COPY cache-node/pom.xml ./cache-node/
COPY proxy/pom.xml ./proxy/

COPY common/src ./common/src
COPY cache-node/src ./cache-node/src

RUN mvn dependency:go-offline -B -pl cache-node -am
RUN mvn clean package -pl cache-node -am -DskipTests

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=builder /app/cache-node/target/cache-node-1.0-SNAPSHOT.jar app.jar

RUN mkdir -p /data

EXPOSE 7000 7001 7002 7100 7101 7102 8081

ENV NODE_ID=node-1
ENV NODE_PORT=7000
ENV REPLICATION_PORT=7100
ENV METRICS_PORT=8081
ENV IS_MASTER=true

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
