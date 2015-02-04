echo "createHTable"
curl -H "Content-Type: application/json" -d '{"tableName":"table9909","columnFamilys":["cf"]}' http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/hbaseadmin/create
echo ""
echo "dropHTable"
curl -i -H "Content-Type: application/json" -X DELETE http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/hbaseadmin/drop/table9909
echo ""