# Multi-stage build: compile with Maven, run on slim JRE

# Build stage
FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /app

# Copy pom first to leverage Docker layer cache for dependencies
COPY pom.xml ./
RUN mvn -B -DskipTests dependency:go-offline

# Copy sources and build
COPY src ./src
RUN mvn -B -DskipTests package \
	&& JAR_FILE=$(ls target/*.jar | grep -v '\.original$' | head -n 1) \
	&& cp "$JAR_FILE" /app/app.jar

# Runtime stage
FROM eclipse-temurin:17-jre-jammy AS runtime
WORKDIR /app

# Copy the fat jar from the build stage
COPY --from=build /app/app.jar /app/app.jar

# Configure runtime
ENV JAVA_OPTS=""
EXPOSE 8080

# Use exec form; JAVA_OPTS can be provided via environment
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
