FROM apache/spark:3.5.5

WORKDIR /app/src
USER root

RUN apt-get -o Acquire::Check-Valid-Until=false update -y \
    && apt-get install -y git vim libc-dev curl wget \
    && apt-get install -y iputils-ping telnet openssh-client \
    && apt-get install -y net-tools man unzip \
    && apt-get install -y vim-tiny bc openssh-server thrift-compiler netcat sudo\
    && apt-get install -y build-essential default-jdk \
    && apt-get autoremove -y\
    && apt-get clean

# Install Python 3.10
RUN apt-get install -y software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update -y \
    && apt-get install -y python3.10 python3.10-dev python3.10-venv python3.10-distutils \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 \
    && update-alternatives --set python3 /usr/bin/python3.10 \
    && apt-get clean

# Upgrade pip
RUN wget https://bootstrap.pypa.io/get-pip.py \
    && python3 get-pip.py \
    && rm get-pip.py


RUN pip install --upgrade pip



COPY requirements.txt .

RUN git config --global http.sslVerify false \
    && pip install -r requirements.txt

ENV PYTHONPATH="./src:./shared_storage:${PYTHONPATH}"

CMD sleep infinity



