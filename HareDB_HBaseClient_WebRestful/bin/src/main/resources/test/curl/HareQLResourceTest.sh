echo "testHareQLSelect"
curl -H "Content-Type: application/json" -d '{"tempFilePath":"/temp/ddd","sql":"select * from table81","page":1,"limit":10}' http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/hareql/query
echo ""
echo "testHareQLInsert"
curl -H "Content-Type: application/json" -d '{"sql":"insert into table81(:key, cf:column1) values(\u0027rowkey101\u0027, \u0027value100\u0027)","page":1,"limit":10}
' http://localhost:8080/HareDB_HBaseClient_WebRestful/webapi/hareql/query
echo ""
