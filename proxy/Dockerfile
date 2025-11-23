FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml ./
COPY common/pom.xml ./common/
COPY cache-node/pom.xml ./cache-node/
COPY proxy/pom.xml ./proxy/

COPY common/src ./common/src
COPY proxy/src ./proxy/src

RUN mvn clean package -pl proxy -am -DskipTests

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=builder /app/proxy/target/proxy-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8080

ENV SERVER_PORT=8080
ENV CACHE_NODES=master-1:cache-master-1:7000:7100,master-2:cache-master-2:7001:7101,master-3:cache-master-3:7002:7102

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
