#!/bin/bash

if [ $# -ne 2 ]; then
  echo $0 usage: ./start_house_data_receivers.sh starting_hid ending_hid
  exit 1
fi

STARTING_HID=$2
ENDING_HID=$3

BASE_PORT_NUMBER=5000

mkdir -p log/house_data_receivers

for i in `seq $STARTING_HID $ENDING_HID`;
do
  log_file='log/house_data_receivers/receiver_house_id_'$i'.log'
  PORT=$(( $BASE_PORT_NUMBER + $i))
  echo 'Starting house data receiver for house id:'$i
  python src/benchmark/house_data_receiver.py $i $PORT >$log_file 2>&1 &
done
