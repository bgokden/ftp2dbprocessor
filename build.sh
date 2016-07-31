#!/usr/bin/env bash

mvn clean package -DskipTests

docker build -t berkgokden/ftpprocessor .
