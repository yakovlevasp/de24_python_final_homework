FROM apache/airflow:2.10.4

USER root
RUN apt-get update
RUN apt-get install -y --no-install-recommends procps
RUN apt-get install -y --no-install-recommends openjdk-17-jre-headless
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY constraints.txt constraints.txt
COPY requirements.txt requirements.txt

RUN pip install pip --upgrade
RUN pip install -r requirements.txt --constraint constraints.txt