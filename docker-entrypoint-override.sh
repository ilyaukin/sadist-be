#!/bin/sh

sh -c "/nginx-reloader.sh &"
./docker-entrypoint.sh "$@"
