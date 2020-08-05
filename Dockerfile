FROM openjdk:8
 
WORKDIR /app

COPY target/DataFastLaneApp-2.0.0-SNAPSHOT-jar-with-dependencies.jar /app/dflapp.jar

ENTRYPOINT ["java", "-jar", "/app/dflapp.jar"]
