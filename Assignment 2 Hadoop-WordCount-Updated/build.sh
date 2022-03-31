#!/bin/bash

mvn clean package

# cp target/wordcount-3.2.1.jar wordcount.jar
cp target/wordcount-3.2.1-jar-with-dependencies.jar wordcount.jar
