# similake

## Running with Docker
To run the application using Docker, follow these steps:


Run the Docker container:

```sh
  docker run -p 6767:6767 tinumistry/similake
```
   
 
foe mac os
```
docker run -p 6767:6767 tinumistry/similakem2


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

