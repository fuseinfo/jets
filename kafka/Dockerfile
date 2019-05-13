FROM centos:centos7
RUN yum -y update && yum -y install java-1.8.0-openjdk-devel && yum -y clean all
ENV JAVA_HOME /usr/lib/jvm/java
COPY target/jets-kafka_2.11-0.2.0-jar-with-dependencies.jar /app/jets-kafka.jar
ENTRYPOINT ["/usr/bin/java", "-jar", "/app/jets-kafka.jar"]
