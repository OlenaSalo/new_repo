#ARG PROJECT_ID=gcrProjectIdPlaceHolder
FROM us.gcr.io/eops-gcr-reg-npe-9959/openjdk:8-jre-alpine

# remove the SUID from all binaries (none in alpine)
RUN for file in `find / -not \( -path /proc -prune \) -type f -perm /6000`; \
      do \
        echo "remove SUID for $file"; \
        chmod a-s $file; \
      done

# Create a non-root user to run the app
ENV USER=appuser UID=1000 GID=1000
RUN addgroup -g ${GID} ${USER} && \
    adduser -u ${UID} ${USER} \
      -G ${USER} \
      -D \
      -S \
      -g ""
USER ${USER}

#Arguments for Build Metadata
ARG GIT_COMMIT
ARG GIT_URL
ARG GIT_BRANCH
ARG BUILD_NUMBER
ARG BUILD_URL

LABEL maintainer="cicd.abcm@equifax.com" \
      GIT_COMMIT="${GIT_COMMIT}" \
      GIT_URL="${GIT_URL}" \
      GIT_BRANCH="${GIT_BRANCH}" \
      BUILD_NUMBER="${BUILD_NUMBER}" \
      BUILD_URL="${BUILD_URL}"

WORKDIR /app

# Argument for Jar file
ARG JAR_FILE

COPY target/${JAR_FILE} /app/helloworld.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/helloworld.jar"]
