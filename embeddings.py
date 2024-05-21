from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import logging        #Logging is a means of tracking events that happen when some software runs
from elasticsearch import logger as elasticsearch_logger 
import json
elasticsearch_logger.setLevel(logging.DEBUG)   

def get_all_ids(index_name, username, password, host='https://spasv2es8stgext.stage.adobesearch.io', port=443, use_ssl=True):
    es = Elasticsearch(
         "https://spasv2es8stgext.stage.adobesearch.io:443",
         basic_auth=(username, password))

    # Use the scan helper to iterate through all documents in the index
    #results = scan(es, query='{"fields": "_id"}', index=index_name, scroll='10s')
    results = scan(es, index=index_name, query={ 
		"_source": {
			"includes": [
				"field_string_search_store_keyword_16",
				"object_embedding_1024_1",  
				"object_embedding_768_1",
			]
		},
		"query": {
			"bool": {
				"filter": [
					{
						"bool": {
							"must": [
								{
									"exists": {
										"field": "object_embedding_768_1.field_double_nonsearch_nonstore_dense_vector_dims_768_1"
									}
                                    
								}
							],
							"boost": 1
						}
					},
					{
						"term": {
							"field_string_search_store_keyword_11": {
								"value": "ACTIVE",
								"boost": 1
							}
						}
					}
				],
				"boost": 1
			}
		}
	},scroll="25m",raise_on_error=False)
    # Extract and return the list of _id values
    li = set()
    count=100
    i = 0
    final_result = {}
    for result in results:
        if not ("_source" in result and "object_embedding_1024_1" in result['_source'] and "object_dynamic_rankerPositiveScoreImpact_1" in result['_source']['object_embedding_1024_1']):
            continue
        asset_id = result['_id']
        embedding1 = result['_source']['object_embedding_1024_1']
        embedding2 = result['_source']['object_embedding_768_1']
        final_result[asset_id] = {
            "embedding1": embedding1,
            "embedding2": embedding2
        }
        i = i+1
        if i == count:
            break

    output_file = "allActiveVideoIdsProd.txt"
    try:
        with open(output_file, 'w') as file:
            file.write(json.dumps(final_result))
    finally:
        print("done")
    return li

# Example usage:
index_name = "hz_templates_es_video_19april"
# username=""
# password=""
username = ""
password = ""
all_ids = get_all_ids(index_name, username, password)
print(all_ids)