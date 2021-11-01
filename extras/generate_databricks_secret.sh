#!/usr/bin/env bash

# Generate a tentaclio databricks secret

echo "Enter your Databricks peronsal access token"
echo "Hint: begins with dapi"
read token

echo "Enter your Databricks instance"
echo "E.g. https://<databricks-instance>.cloud.databricks.com/"
read instance

echo "Enter your SQL endpoint"
echo "E.g. /sql/1.0/endpoints/1a2b3c4d5e6f7g"
read endpoint

echo "Enter your simba driver path"
echo "Macos default: /Library/simba/spark/lib/libsparkodbc_sbu.dylib"
echo "Linux default: /opt/simba/spark/lib/64/libsparkodbc_sb64.so"
read driver

echo "Your databricks secret is:"
printf "databricks+pyodbc://%s@%s.cloud.databricks.com:443/?HTTPPath=\
%s&AuthMech=3&SparkServerType=3&ThriftTransport=2&SSL=1\
&IgnoreTransactions=1&DRIVER=%s" $token $instance $endpoint $driver

