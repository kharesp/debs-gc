import zmq

context=zmq.Context()
subscriber=context.socket(zmq.PULL)
subscriber.set_hwm(1000000)
subscriber.bind('tcp://*:5000')
while True:
  reading=subscriber.recv_string()
  print(reading)
