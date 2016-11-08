#!/bin/bash

if [ $# -ne 2 ]; then
  echo $0 usage: ./run_processors.sh server_address num_houses
  exit 1
fi

ADDRESS=$1
NUM_HOUSES=$2

log_file='log/house_processor.log'
python src/run_processors.py $ADDRESS $NUM_HOUSES  > $log_file 2>&1 &
