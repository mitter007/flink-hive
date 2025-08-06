#!/bin/bash

#在执行前打印脚本命令
set -x

#kafka kerberos认证
kinit -kt /home/tomcat/keytab-util/fid_bg_hlrs.keytab


/bin/flink run \
-m yarn-cluster -yqu root.job \
 -ynm hlrs_flink_2hdfs -ys 5 \
  -ytm 8192m
-ytm
-yjm  8192m -d -c

--flink_hdfs_config_path ./hlrs_hdfs.properties