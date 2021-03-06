# Builder part for maven packaging.
FROM maven:3-openjdk-8 as builder

WORKDIR /app

# First download as many dependencies as possible. This avoids downloading all dependencies each time something in source changes
COPY pom.xml /app/
COPY taskmanager/pom.xml /app/taskmanager/
COPY taskmanager-client/pom.xml /app/taskmanager-client/
RUN mvn package clean -Dmaven.main.skip -Dmaven.test.skip

# Next do the actual build
COPY . /app
RUN mvn clean package

# Move the resultant jar to a known name so it can be picked up by the final image.
RUN mv -v "$(ls taskmanager/target/taskmanager-*.jar | grep -v -e '-javadoc' -e '-sources')" app.jar \
# Clean up image so we can rerun without the cache being invalidated because target files are changed
    && mvn clean

# Final image containing only the application
FROM openjdk:11-jre-buster

WORKDIR /app

# The following ENV variables are set by default. Overwrite them to suit your configuration.
ENV AERIUS_BROKER_HOST="localhost" \
    AERIUS_BROKER_PORT="5672" \
    AERIUS_BROKER_USERNAME="aerius" \
    AERIUS_BROKER_PASSWORD="aerius"

# Make queue directory
RUN mkdir /app/queue

COPY --from=builder /app/app.jar /app/
COPY taskmanager/src/main/docker/taskmanager.properties /app/

ENTRYPOINT ["java","-jar","app.jar", "-config", "taskmanager.properties"]
