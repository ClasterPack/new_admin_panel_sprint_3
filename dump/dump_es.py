import asyncio
import json

from elasticsearch import AsyncElasticsearch


# Elasticsearch connection
async def dump_from_elasticsearch(index_name: str, output_file: str):
    # Elasticsearch URL using http://0.0.0.0:9200 (or localhost/127.0.0.1)
    elastic_url = 'http://localhost:9200'  # or use 'http://0.0.0.0:9200' if appropriate

    # Initialize Elasticsearch client
    elastic = AsyncElasticsearch(hosts=[elastic_url])

    # Initialize an empty list to store documents
    documents = []

    # Perform the initial search query
    response = await elastic.search(index=index_name, scroll="2m", size=1000)
    scroll_id = response["_scroll_id"]  # Save the scroll ID for subsequent requests

    # Collect hits from the first batch of results
    documents.extend(response['hits']['hits'])

    # Use the scroll API to retrieve the rest of the data in batches
    while len(response['hits']['hits']) > 0:
        # Perform scroll query
        response = await elastic.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = response["_scroll_id"]  # Update scroll ID for the next scroll
        documents.extend(response['hits']['hits'])  # Add new hits to our documents list

    # Close the Elasticsearch connection
    await elastic.close()

    # Write the collected documents to a JSON file
    with open(output_file, 'w') as f:
        json.dump(documents, f, indent=4)

    print(f"Data from index '{index_name}' has been dumped to '{output_file}'")

# Run the dump function asynchronously
async def main():
    # Specify the Elasticsearch index name and the output file name
    await dump_from_elasticsearch("movies", "dumped_data.json")

if __name__ == "__main__":
    asyncio.run(main())


