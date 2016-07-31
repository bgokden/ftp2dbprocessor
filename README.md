# ftp2dbprocessor

This is an ETL system to download and process weather data files from an ftp server.

This system has 4 parts:

An observer: Track ftp server for newer files and post filenames to a work queue

A work queue: I have chosen Rabbit MQ as AMQP implementation

A worker: Checks work queue for filenames, then download, process and store data to db

A db: Elasticsearch is used as a db. Elasticsearch should be started as a separete service.

Docker and docker-compose should be installed to use this project

To run the program execute the commands:

Optionally, you can build compile and build this project

    ./build.sh

If you don't have an elasticsearch service, you can start with this docker command:

    docker run --rm -p 9200:9200 -p 9300:9300 elasticsearch

Set environment variables like ftp server settings and elasticsearch settings in set-env.sh
and source the environment:

    source set-env.sh

Note: ELASTICSEARCHHOST should include comma seperated hosts like server1:port1,server2:port2

Then run the whole system with docker-compose:

    docker-compose up

Main scalability issue lies in the number of workers.
To increase number of workers to 3:

    docker-compose scale worker=3

To run the tests execute the command:

    mvn test

To run the sonar execute the command:

    mvn sonar:sonar

Before running sonar start sonar service (as docker command):

    docker run -d --name sonarqube -p 9000:9000 -p 9092:9092 sonarqube
