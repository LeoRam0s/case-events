FROM eclipse-temurin:21-jdk AS build

WORKDIR /app
COPY mvnw pom.xml ./
COPY .mvn/ .mvn/
COPY src/ src/
RUN ./mvnw -Dmaven.test.skip=true package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /app/target/intelbras-event-engine-*.jar app.jar

EXPOSE 3000
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
