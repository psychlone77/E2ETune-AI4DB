#!/bin/bash
sudo apt install openjdk-21-jdk

git clone https://github.com/cmu-db/benchbase.git

cd benchbase

git checkout b4b36683afdf79dd8bf70b95199b874c68218975

./mvnw clean package -P $1

cd target
tar xvzf benchbase-$1.tgz
