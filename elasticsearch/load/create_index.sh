curl -XPOST localhost:9200/bench -d '{
    "settings" : {
        "number_of_shards" : 16,
		"number_of_replicas" : 0,
    }
}'
