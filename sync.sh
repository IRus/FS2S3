#!/bin/bash

./gradlew clean shadowJar

source ./env.sh

java -jar ./build/libs/fs2s3.jar "$1" "$2"

