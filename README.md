# similake

## Running with Docker
To run the application using Docker, follow these steps:


Run the Docker container:

```sh
  docker run -p 6767:6767 tinumistry/similake
```
   


## add new vector store using rest api

```
curl --location --request PUT 'http://localhost:6767/collections/vector_store' \
--header 'api-key: 1234' \
--header 'Content-Type: application/json' \
--data '{
    "size": 1536,
    "distance": "Cosine",
    "persist" : "true"
  }'
```

## delete vector store using rest api

```
curl --location --request DELETE 'http://localhost:6767/collections/vector_store' \
--header 'api-key: 1234' \
--header 'Content-Type: application/json' \
--data '{
    "size": 1536,
    "distance": "Cosine",
    "persist" : "true"
  }'
```
## Add Document to Vector Store
Add a document with embedded vector representation to the collection.

POST http://localhost:6767/collections/vector_store/payload
### Request Body
```json
{
  "id": "1e2c9361-a9a5-4f85-bd83-75957067eb3b",
  "metadata": {
    "brand": "Apple", 
    "product_name": "Smartphone"
  },
  "content": "Apple Latest model smartphone with advanced camera features and a powerful processor.",
  "media": [],
  "embedding": [
    -0.009170532,
    0.02229309,
    0.026123047,
    ...
    0.024642944,
    0.0032863617
  ]
}

