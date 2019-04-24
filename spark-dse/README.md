# Spark + DSE Graph Examples

## Dependencies

* [Docker / Docker-Compose](https://docs.docker.com/compose/install/)
* [Maven](https://maven.apache.org/install.html)
* Java (tested with Java 8)

## Run the examples as tests

```
# Create a Docker network
docker network create graph

# Start the Docker services
docker-compose up -d

# Wait until the DSE container starts. Should take a few minutes.
# You can check the DSE startup logs with `docker-compose logs -f dse`

# Start the DSE node with Docker
mvn clean test
```

## View the example code

All examples are in the `src/test/scala/org/mjm` directory.

## Use DSE Studio

Open a browser to http://localhost:9091

Refer to the [DataStax docs](https://docs.datastax.com/en/studio/6.7/index.html) for more info on DSE Studio.

NOTE: [this site](http://www.luketillman.com/datastax-graph-and-studio-with-docker-compose/) provides information on how to configure DSE Studio to connect to the DSE docker container.