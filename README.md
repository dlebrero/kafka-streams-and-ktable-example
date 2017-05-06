# Kafka Streams and KTables examples

This code is the companion of the blog post [Joins on stateful stream processing using Kafka Streams' KTables and GlobalKTables](http://danlebrero.com/2017/05/07/kafka-streams-ktable-globalktable-joining-reference-data/)

This project uses Docker to create a cluster of 3 microservices that consume from a Kafka topic using the
Kafka Streams API.

The main processing function is [here](our-service/src/our_service/kafka_streams.clj#L146).

## Usage

Docker should be installed.

To run:

     cd our-service
     docker build . -t danlebrero/ktable-demo
     cd ..
     docker-compose -p ktable-demo -f docker-compose.yml up
     
Once the environment has been started, you have to add some reference data first:

     curl --data "ticker=AAPL&exchange=NASDAQ&name=Apple" -X POST http://localhost:3004/set-ref-data


Then you have to add the client data, in this case just the email:

     curl --data "client=client1&email=foo@bar.com" -X POST http://localhost:3004/set-client-data

And then you can add new positions with:

     curl --data "position=position1&client=client1&amount=1&ticker=AAPL" -X POST http://localhost:3004/set-shares

You can now see what is the local state of a container:

     curl http://localhost:3004/local-state  

To remove a position, just set the amount to 0:

     curl --data "position=position1&amount=0" -X POST http://localhost:3004/set-shares
     
## Clean up

To get rid of all:

    docker-compose -p ktable-demo -f docker-compose.yml down --rmi all --remove-orphans
    docker image rm pandeiro/lein:2.5.2 wurstmeister/kafka:0.10.1.0-1
    
    
    
