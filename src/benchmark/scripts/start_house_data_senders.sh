#!/bin/bash


if [ $# -ne 3 ]; then
  echo $0 usage: ./start_house_data_senders.sh server_address starting_hid ending_hid
  exit 1
fi

SERVER_ADDRESS=$1
STARTING_HID=$2
ENDING_HID=$3

BASE_PORT_NUMBER=5000

mkdir -p log/house_data_senders

for i in `seq $STARTING_HID $ENDING_HID`;
do
  log_file='log/house_data_senders/sender_house_id_'$i'.log'
  PORT=$(( $BASE_PORT_NUMBER + $i))
  echo 'Starting house data sender for house id:'$i
  python src/benchmark/house_data_sender.py $i $SERVER_ADDRESS $PORT > $log_file &
done
