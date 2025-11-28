FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml ./
COPY common/pom.xml ./common/
COPY cache-node/pom.xml ./cache-node/
COPY proxy/pom.xml ./proxy/

COPY common/src ./common/src
COPY proxy/src ./proxy/src

RUN mvn dependency:go-offline -B -pl proxy -am
RUN mvn clean package -pl proxy -am -DskipTests

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=builder /app/proxy/target/proxy-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
