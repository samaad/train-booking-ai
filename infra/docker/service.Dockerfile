# Shared image for every Spring Boot service. Expects the jar to be built on the host first
# (./mvnw package) — keeps image builds seconds-fast instead of re-downloading Maven deps.
# JVM mode only; no GraalVM native (see .claude/plan/08-deploy.md).

FROM eclipse-temurin:25-jre AS extract
ARG SERVICE
ARG MODULE_DIR=services
WORKDIR /build
COPY ${MODULE_DIR}/${SERVICE}/target/${SERVICE}-*.jar app.jar
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination extracted

FROM eclipse-temurin:25-jre
# Numeric UID so Kubernetes can enforce runAsNonRoot (it cannot verify a user name).
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app app
USER 10001:10001
WORKDIR /app
# Least- to most-frequently changing, so dependency layers stay cached.
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
# Netty loads native transports; JDK 25 warns unless native access is granted.
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "org.springframework.boot.loader.launch.JarLauncher"]
