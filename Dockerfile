FROM ubuntu:latest

# Install OpenJDK 8
RUN \
  apt-get update && \
  apt-get install -y openjdk-8-jdk && \
  rm -rf /var/lib/apt/lists/*

# Install Python
RUN \
    apt-get update && \
    apt-get install -y python3 python3-dev python3-pip python3-virtualenv && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark and kaggle
RUN \
    pip3 install --upgrade pip && \
    pip3 install kaggle && \
    pip3 install pyspark

# Define working directory
WORKDIR /data

# Define default command
CMD ["bash"]