FROM centos

ENV LC_ALL C.UTF-8

COPY requirements.txt /tmp/
COPY bigtable_export.py bigtable_import.py /usr/local/bin/

RUN yum --assumeyes install  python3-pip java-1.8.0-openjdk-headless \
    && yum --assumeyes autoremove \
    && yum --assumeyes clean all \
    && rm -rf /var/lib/yum/yumdb/

RUN pip3 install --no-cache-dir --upgrade pip setuptools
RUN pip install --no-cache-dir --requirement /tmp/requirements.txt

ARG bt_jar_ver=1.14.0
RUN curl --progress-bar --output /usr/local/lib/bigtable-beam-import.jar \
https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-beam-import/${bt_jar_ver}/bigtable-beam-import-${bt_jar_ver}-shaded.jar
