package main

import (
	"encoding/json"
	"github.com/linkedin/goavro/v2"
	"io/ioutil"
	"math/rand"
	"time"
)

const(
	conversationValueKey = "\"rcrai_mid_db.rcrai.rcrai_conversations.Value\""
	conversationNonValues=`"source": {
    "connector": "mysql",
    "db": "rcrai",
    "file": "mysql-bin.000003",
    "gtid": {
      "string": "81df1746-f663-11ea-862d-0242ac140007:37"
    },
    "name": "rcrai-mid-db",
    "pos": 12473,
    "query": null,
    "row": 0,
    "server_id": 321541,
    "snapshot": {
      "string": "false"
    },
    "table": {
      "string": "rcrai_conversations"
    },
    "thread": {
      "long": 42
    },
    "ts_ms": 1600235435000,
    "version": "1.1.0.Final"
  },
  "op":"c",
  "transaction": null,
  "ts_ms": {
    "long": 1600235435303
  }`
)

func main() {
	conversationDesc    :=`{"type":"record","name":"Envelope","namespace":"rcrai_mid_db.rcrai.rcrai_conversations","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"conversation_id","type":"string"},{"name":"url","type":"string"},{"name":"start_time","type":{"type":"long","connect.version":1,"connect.name":"io.debezium.time.Timestamp"}},{"name":"category","type":"string"},{"name":"roles","type":["null","string"],"default":null},{"name":"is_initial","type":{"type":"int","connect.type":"int16"}},{"name":"staff_id","type":"string"},{"name":"customer_id","type":"string"},{"name":"priority","type":[
"int","null"
],"default":9},{"name":"create_time","type":[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"null"],"default":0},{"name":"update_time","type":[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"null"],"default":0},{"name":"version","type":"string"}],"connect.name":"rcrai_mid_db.rcrai.rcrai_conversations.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"rcrai_mid_db.rcrai.rcrai_conversations.Envelope"}`
	codec, err := goavro.NewCodec(conversationDesc)
	if err != nil {
		panic(err)
	}

	//native, _, err := codec.NativeFromTextual([]byte(newConversationValue()))
	//if err != nil {
		//panic(err)
	//}
	v := newConversationValue()
	completeValue := `{"after":{`+conversationValueKey + ":" + vToJson(v) + `}, "before":null,` + conversationNonValues +`}`
	native, _,  err := codec.NativeFromTextual([]byte(completeValue))
	if err != nil {
		panic(err)
	}
	(native.(map[string]interface{}))["source"] = "anything"
	buf, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		panic(err)
	}
	native, _, err = codec.NativeFromBinary(buf)
	if err != nil {
		panic(err)
	}

	{
		buf, _ := ioutil.ReadFile("/home/jan/avro.dump")
		native, _, err = codec.NativeFromBinary(buf)
		if err != nil {
			panic(err)
		}
	}
}

func vToJson(v map[string]interface{}) string{
	buf, _:=json.Marshal(v)
	return string(buf)
}


func newConversationValue() map[string]interface{}{
	/* {
	     "category": "category002",
	     "conversation_id": "conversation002",
	     "create_time": {
	       "long": 0
	     },
	     "customer_id": "customer002",
	     "is_initial": 0,
	     "priority": {
	       "int": 5
	     },
	     "roles": {
	       "string": "c,s"
	     },
	     "staff_id": "staff002",
	     "start_time": 1600264085000,
	     "update_time": {
	       "long": 0
	     },
	     "url": "http://myhost/url",
	     "version": "1.0"
	   }
	*/
	var value =make(map[string]interface{})
	value["url"]= "http://myhost/url"
	value["roles"]=map[string]interface{}{
		"string":"c,s",
	}
	value["category"]="uuid.New().String()"
	value["version"]="1.0"
	value["create_time"] = map[string]interface{}{
		"long":time.Now().Unix() * 1000,
	}
	value["update_time"] = map[string]interface{}{
		"long":time.Now().Unix() * 1000,
	}
	value["is_initial"] = map[bool]interface{}{true:int32(1),false:int32(0)}[rand.Float64() > 0.5]
	value["conversation_id"] = "uuid.New().String()"
	value["customer_id"] = "uuid.New().String()"
	value["staff_id"] = "uuid.New().String()"
	value["start_time"]=time.Now().UnixNano() / 1000 / 1000
	value["priority"] = map[string]interface{}{
		"int":int32(rand.Intn(1000000)),
	}
	return value
}