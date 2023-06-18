#!/bin/sh

while true; do
  inotifywait -e create -e modify -e delete -e move /etc/letsencrypt/live/my-handicapped-pet.io/
  echo "Reloading nginx..."
  nginx -s reload
done
