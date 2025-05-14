import asyncio
import json

from elasticsearch import AsyncElasticsearch, helpers


# Elasticsearch connection
async def dump_to_elasticsearch(input_file: str, index_name: str):
    # Elasticsearch URL (make sure to use the correct URL)
    elastic_url = "http://localhost:9200"  # Replace with your Elasticsearch URL

    # Initialize Elasticsearch client
    elastic = AsyncElasticsearch(hosts=[elastic_url])

    # Open the JSON file containing the dumped data
    with open(input_file, "r") as f:
        documents = json.load(f)

    # Prepare actions for bulk indexing
    actions = []
    for doc in documents:
        # Each action is a dictionary containing the index operation
        action = {
            "_op_type": "index",  # Operation type (could also be "create", "update", "delete")
            "_index": index_name,  # The index name where you want to insert the data
            "_id": doc[
                "_id"
            ],  # Optional: specify _id if you want to retain the original IDs
            "_source": doc["_source"],  # The document source (data)
        }
        actions.append(action)

    # Perform bulk indexing (faster than indexing each document individually)
    success, failed = await helpers.async_bulk(elastic, actions)
    print(f"Successfully indexed {success} documents.")
    if failed:
        print(f"Failed to index {failed} documents.")

    # Close the Elasticsearch client
    await elastic.close()


# Example usage
async def main():
    # Specify the input JSON file and target Elasticsearch index
    await dump_to_elasticsearch("dumped_data.json", "movies")


# Run the function asynchronously
if __name__ == "__main__":
    asyncio.run(main())
