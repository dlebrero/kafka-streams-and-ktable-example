# Kafka Streams and KTables examples

This code is the companion of the blog post [Proof of concept using KafkaStreams and KTables](http://danlebrero.com/2017/01/05/proof-of-concept-using-kafkastreams-and-ktables/). 

There is a longer explanation of the code at [Proof of concept using KafkaStreams and KTables - implementation notes, gotchas and Docker Compose example](http://danlebrero.com/2017/01/06/proof-of-concept-using-kafkastreams-and-ktables-implementation-notes-gotchas-and-docker-compose/)

This project uses Docker to create a cluster of 3 microservices that consume from a Kafka topic using the
Kafka Streams API.

The main processing function is [here](our-service/src/our_service/kafka_streams.clj#L55).

## Usage

Docker should be installed.

To run:

    cd our-service
    docker build . -t danlebrero/ktable-demo
    cd ..
    docker-compose -p ktable-demo -f docker-compose.yml up
     
Once the environment has been started, you can add new positions with:

    curl --data "client=client1&exchange=NASDAQ&amount=1&ticker=AAPL" -X POST http://localhost:3004/set-shares

To remove a position, just set the amount to 0:

    curl --data "client=client1&exchange=NASDAQ&amount=0&ticker=AAPL" -X POST http://localhost:3004/set-shares
     
## Clean up

To get rid of all:

    docker-compose -p ktable-demo -f docker-compose.yml down --rmi all --remove-orphans
    docker image rm pandeiro/lein:2.5.2 wurstmeister/kafka:0.10.1.0-1
