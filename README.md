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
 
    0.024642944,
    0.0032863617
  ]
}
```
### Bulk Upload Endpoint

```http
POST /collections/{vectorName}/payloads/bulk
```

#### Request Body Example

```json
[
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
      0.024642944
    ]
  },
  {
    "id": "2f3d8472-b8b6-5g96-ce94-86068178fc4c",
    "metadata": {
      "brand": "Samsung",
      "product_name": "4K Smart TV"
    },
    "content": "Samsung 65-inch 4K Ultra HD Smart TV with quantum processor.",
    "media": [],
    "embedding": [
      0.018256743,
      -0.012345678,
      0.031234567,
      0.027834561
    ]
  },
  {
    "id": "3g4e9583-c9c7-6h07-df05-97079289gd5d",
    "metadata": {
      "brand": "Sony",
      "product_name": "Headphones"
    },
    "content": "Sony wireless noise-canceling headphones with long battery life.",
    "media": [],
    "embedding": [
      0.023456789,
      0.034567890,
      -0.015678901,
      0.028901234
    ]
  }
]
```

#### cURL Example

```bash
curl -X POST \
  'http://localhost:8080/collections/my-vector-store/payloads/bulk' \
  -H 'Content-Type: application/json' \
  -d @payload.json
```

#### Response

```json
{
  "message": "Successfully added 3 payloads to vector-store",
  "status": 201
}
```