FROM java:8-jre

ADD /target/ftp2dbprocessor-1.0-SNAPSHOT.jar /etc/ftp2dbprocessor.jar

ENTRYPOINT ["java", "-jar", "/etc/ftp2dbprocessor.jar"]
