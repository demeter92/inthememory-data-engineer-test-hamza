FROM apache/hive:4.0.0

# Install curl
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/hive/lib

# Download JAR files from the internet
RUN curl -o postgres.jar https://jdbc.postgresql.org/download/postgresql-42.5.4.jar && \
    curl -o hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -o aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Set working directory back to Hive home
WORKDIR /opt/hive

# Copy metastore configuration file
COPY metastore-site.xml /opt/hive/conf/metastore-site.xml
