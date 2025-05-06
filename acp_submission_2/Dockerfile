FROM openjdk:21
# FROM openjdk
VOLUME /tmp
EXPOSE 8080

COPY target/AcpCw2Template*.jar app.jar
ENTRYPOINT ["java", "-jar","/app.jar"]