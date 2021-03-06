#!/bin/bash

if [ $# -ne 2 ]; then
  echo $0 usage: ./run_processors.sh num_houses win_size
  exit 1
fi

NUM_HOUSES=$1
WIN_SIZE=$2

mkdir -p log

house_processor_log_file='log/house_processor.log'
metrics_log_file='log/metrics.log'

echo 'Starting Metrics receiver'
python src/recieve_metrics.py $WIN_SIZE  > $metrics_log_file 2>&1 &

sleep 2

echo 'Starting House Processor for ' $NUM_HOUSES 'houses'
python src/start_processors.py $NUM_HOUSES  > $house_processor_log_file 2>&1 &
