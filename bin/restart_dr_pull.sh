#!/bin/usr/env bash

while IFS='=' read -r key value; do
  if [ "$key" = "DR_THREAD" ]; then
    n=$value

    i=1
    while [ $i -le $n ]
    do
      sudo systemctl restart dr_pull@"thread_$i".service
      sleep 1
      i=$((i+1)) 
    done
  fi
done < ".env"
