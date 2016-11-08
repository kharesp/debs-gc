import zmq, common, rx
   
def data_stream(server_address):
  def read(observer):
    context=zmq.Context()
    subscriber=context.socket(zmq.PULL)
    subscriber.set_hwm(common.hwm)
    subscriber.bind(server_address)
    while True:
      reading=subscriber.recv_pyobj()
      observer.on_next(reading)
  
  return rx.Observable.create(read)
