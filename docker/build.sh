#!/bin/bash

docker build -t companion_rf:1.0 ./companion_rf
docker tag companion_rf:1.0 companion_rf:latest
