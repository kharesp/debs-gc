import zmq,common,time,json

if __name__=="__main__":
  context=zmq.Context()
  publisher=context.socket(zmq.PUSH)
  publisher.set_hwm(common.hwm)
  publisher.connect(common.address)

  with open(common.data_file) as f:
    for line in f:
      values= line.split(',')
      reading= common.Reading(id= int(values[0]),
        ts= int(values[1]),
        value= float(values[2]),
        property= int(values[3]),
        plug_id= int(values[4]),
        hh_id= int(values[5]),
        h_id= int(values[6]))

      publisher.send_pyobj(reading)
      
  publisher.close()
