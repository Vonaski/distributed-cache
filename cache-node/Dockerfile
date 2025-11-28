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

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
