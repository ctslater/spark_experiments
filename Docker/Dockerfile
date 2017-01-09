FROM java:openjdk-8-jdk

ENV hadoop_ver 2.7.3
ENV spark_ver 2.1.0

# Get Hadoop from US Apache mirror and extract just the native
# libs. (Until we care about running HDFS with these containers, this
# is all we need.)
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://www.us.apache.org/dist/hadoop/common/hadoop-${hadoop_ver}/hadoop-${hadoop_ver}.tar.gz | \
        tar -zx && \
    ln -s hadoop-${hadoop_ver} hadoop && \
    echo Hadoop ${hadoop_ver} native libraries installed in /opt/hadoop/lib/native

# Get Spark from US Apache mirror.
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://www.us.apache.org/dist/spark/spark-${spark_ver}/spark-${spark_ver}-bin-without-hadoop.tgz| \
        tar -zx && \
    ln -s spark-${spark_ver}-bin-without-hadoop spark && \
    echo Spark ${spark_ver} installed in /opt

RUN curl http://central.maven.org/maven2/org/apache/hadoop/hadoop-openstack/${hadoop_ver}/hadoop-openstack-${hadoop_ver}.jar \
    >/opt/spark/jars/hadoop-openstack-${hadoop_ver}.jar && \
    ln -s /opt/spark/jars/hadoop-openstack-${hadoop_ver}.jar /opt/hadoop/share/hadoop/common/lib/

# workers, so install it everywhere
RUN apt-get update && \
    apt-get install -y python-numpy busybox && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ADD log4j.properties /opt/spark/conf/log4j.properties
RUN echo "export SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)" > /opt/spark/conf/spark-env.sh
ADD start-worker start-master /
ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN ln -s /run/secrets/core-site /opt/spark/conf/core-site.xml
ENV PATH $PATH:/opt/spark/bin
