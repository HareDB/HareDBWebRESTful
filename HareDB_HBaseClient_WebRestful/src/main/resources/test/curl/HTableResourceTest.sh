curl -H "Content-Type: application/json" -d '{"zookeeperHost":"host1","zookeeperPort":"2181","nameNodeHostPort":"hdfs://host1:9000","rmAddressHostPort":"host1:8032","rmSchedulerAddressHostPort":"host1:8030","rmResourceTrackerAddressHostPort":"host1:8031","rmAdminAddressHostPort":"host1:8033","mrJobhistoryAddress":"host1:10020","yarnNodeManagerAuxServices":"mapreduce_shuffle","hiveConnType":"LOCAL","metaStoreConnectURL":"jdbc:mysql://192.168.1.215:3306/metastore_db","metaStoreConnectDriver":"com.mysql.jdbc.Driver","metaStoreConnectUserName":"root","metaStoreConnectPassword":"123456"}' http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/connect
echo "test scan"
curl http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/htable/scan/table81/1/3
echo ""
echo "test put"
curl -H "Content-Type: application/json" -d '{"tableName":"table95","rowkey":"rowkey2","columnFamily":"cf","qualifier":"column1","value":"value1"}' http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/htable/put
echo ""
echo "test delete 1"
curl -i -H "Content-Type: application/json" -X DELETE http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/htable/delete/table95/rowkey2
echo ""
echo "test delete 2"
curl -i -H "Content-Type: application/json" -X DELETE http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/htable/delete/table95/rowkey1/cf
echo ""
echo "test delete 3"
curl -i -H "Content-Type: application/json" -X DELETE http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/htable/delete/table95/rowkey1/cf/column1
echo ""