# Worker image (Java). Built by the app through the Docker API the first time Java workers are used.
# Unit tests have their own file: docker build -f workers/java/Dockerfile.test workers/java
# The Kafka client is the official Apache one for Java; the shared settings are translated to its property names.
FROM maven:3-eclipse-temurin-25 AS build
WORKDIR /src
COPY pom.xml ./
COPY src/main ./src/main
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:25-jre
COPY --from=build /src/target/worker.jar /worker.jar
# The heap is half of the container memory: the rest is needed outside the heap (native compression code, class metadata,
# thread stacks). A heap of 75% made a 256 MB sender run out of memory and get killed by Docker.
CMD ["java", "-XX:MaxRAMPercentage=50", "--enable-native-access=ALL-UNNAMED", "-jar", "/worker.jar"]
