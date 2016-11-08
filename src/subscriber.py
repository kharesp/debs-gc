import zmq, common, rx
   
def data_stream():
  def read(observer):
    context=zmq.Context()
    subscriber=context.socket(zmq.PULL)
    subscriber.set_hwm(common.hwm)
    subscriber.bind(common.address)
    while True:
      reading=subscriber.recv_pyobj()
      observer.on_next(reading)
  
  return rx.Observable.create(read)

if __name__=="__main__":
  data_stream().subscribe(lambda r: print(r.value))
