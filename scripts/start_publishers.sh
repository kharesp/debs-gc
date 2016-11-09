#!/bin/bash

if [ $# -ne 2 ]; then
  echo $0 usage: ./run_publishers.sh server_address num_publishers
  exit 1
fi

ADDRESS=$1
NUM=$2
let NUM--
PORT=5000

mkdir -p log

for i in `seq 0 $NUM`; 
do
  connector_string='tcp://'$ADDRESS':'$PORT
  data_file='data/'$i'.csv'
  log_file='log/pub'$i'.log'
  
  echo 'Started Publisher:'$i
  python src/publisher.py $connector_string $data_file > $log_file 2>&1 &
  let PORT++
done
