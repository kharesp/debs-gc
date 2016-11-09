import zmq

context=zmq.Context()
publisher=context.socket(zmq.PUSH)
publisher.set_hwm(1000000)
publisher.connect('tcp://127.0.0.1:5000')

for i in range(1000):
  publisher.send_string('%d'%i)
