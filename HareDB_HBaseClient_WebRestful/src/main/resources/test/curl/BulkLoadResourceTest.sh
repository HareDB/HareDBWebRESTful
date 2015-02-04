echo "testexecuteBulkload"
curl -H "Content-Type: application/json" -d '{"schemaFilePath":"hdfs://host1:9000/schema.prop","dataPath":"hdfs://host1:9000/aaa.txt"}' http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/bulkload/schema/upload
echo ""