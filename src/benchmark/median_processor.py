import common,time,random,statistics
from pymongo import MongoClient
from functools import reduce

class Processor(object):
  def __init__(self,verbose=False):
    self.client=MongoClient()
    self.verbose=verbose

  def print_msg(self,msg):
    if(self.verbose):
      print(msg)
    
  def process(self,query):
    result=None
    if (query['query_type']== common.QueryType.load.name):
      result=self.load(query)
    elif (query['query_type']== common.QueryType.outlier.name):
      result=self.outlier(query)
    else:
      self.print_msg('query_type:%s not recognized\n'%query['query_type'])
    return result

  def load(self,query):
    start_time=time.time()
    self.print_msg('Received load query for level:%s, granularity:%s for traget:%s,\
      prediction_win_ts:%d and history:%s\n'%\
      (query['query_level'],query['granularity'],\
      query['query_target'],query['prediction_win_ts'],query['history']))

    prediction_window_ts=query['prediction_win_ts']
    self.print_msg("prediction window ts:%d\n"%prediction_window_ts)

    history=query['history']
    start_ts=-1
    end_ts=-1
    if (history== common.History.past_1day.name):
      end_ts=prediction_window_ts-common.step_size
      start_ts=end_ts-(1*common.number_of_steps_in_a_day*\
        common.step_size) 
    elif (history== common.History.past_3days.name):
      end_ts=prediction_window_ts-common.step_size
      start_ts=end_ts-(3*common.number_of_steps_in_a_day*\
        common.step_size + 2*common.step_size)
    elif (history== common.History.past_5days.name):
      end_ts=prediction_window_ts-common.step_size
      start_ts=end_ts-(5*common.number_of_steps_in_a_day*\
        common.step_size + 4*common.step_size)
    else:
      self.print_msg("history length:%s not recognized\n"%(history))
      return None

    self.print_msg("Will train on data with start_ts:%d and end_ts:%d\n"%\
      (start_ts,end_ts))

    Ts=[]
    V=[]
    if(query['query_level']==common.QueryLevel.plug.name):
      Ts,V=\
        self.plug_level_load_prediction(query,start_ts,end_ts)
    elif (query['query_level']==common.QueryLevel.household.name):
      Ts,V=\
        self.household_level_load_prediction(query,start_ts,end_ts)
    elif (query['query_level']==common.QueryLevel.house.name):
      Ts,V=\
        self.house_level_load_prediction(query,start_ts,end_ts)
    else:
      self.print_msg('invalid query level')
      return None
  
    #prediction
    median_container=[]
    t_struct=time.gmtime(prediction_window_ts)
    index=(t_struct.tm_hour*60*60+\
      t_struct.tm_min*60+\
      t_struct.tm_sec)//common.step_size

    last_avg=V[-1]
    for i in range(0,len(Ts)):
      if(Ts[i]==index):
        median_container.append(V[i])


    load_forecast=(last_avg+statistics.median(median_container))/2
    
    
    end_time=time.time()
    elapsed_time=end_time-start_time
    
    self.print_msg("Load Prediction results:\n")
    self.print_msg("Predicted load:%f\nelapsed_time:%f seconds\n"%\
      (load_forecast,elapsed_time))
    return {'mean':load_forecast,\
      'elapsed_time':elapsed_time}
    
    
  def plug_level_load_prediction(self,query,\
    start_ts,end_ts):
    self.print_msg("plug_level_load_prediction called")
    target_plug=query['query_target']
    plug_id,hh_id,h_id=target_plug.split('_')
    cursor=self.client['load_%s'%h_id]['%s_%s'%(hh_id,h_id)].\
      find({"$and": [{"ts": {"$gt": start_ts-common.step_size}},\
      {"ts":{"$lt":end_ts+common.step_size}}]})
    self.print_msg('Retrieved %d samples\n'%cursor.count())
    Ts=[]
    V=[]
    for reading in cursor:
      Ts.append(reading['index'])
      V.append(reading['plugs'][target_plug])

    modified_Ts,modified_V= self.\
      curate_for_granularity(query['granularity'],Ts,V) 

    return modified_Ts, modified_V 

  def household_level_load_prediction(self,query,
    start_ts,end_ts):
    target_hh=query['query_target']
    hh_id,h_id=target_hh.split('_')
    cursor=self.client['load_%s'%h_id][target_hh].\
      find({"$and": [{"ts": {"$gt": start_ts-common.step_size}},\
      {"ts":{"$lt":end_ts+common.step_size}}]})
    self.print_msg('Retrieved %d samples\n'%cursor.count())
    Ts=[]
    V=[]
    for reading in cursor:
      Ts.append(reading['index'])
      V.append(reading['hh_load'])

    modified_Ts,modified_V= self.\
      curate_for_granularity(query['granularity'],Ts,V) 

    return modified_Ts,modified_V

  def house_level_load_prediction(self,query,
    start_ts,end_ts):
    target_house=query['query_target']
    hh_collections=self.client['load_%s'%target_house].collection_names()
    cursors=[self.client['load_%s'%target_house][col].\
      find({"$and": [{"ts": {"$gt": start_ts-common.step_size}},\
      {"ts":{"$lt":end_ts+common.step_size}}]}) \
      for col in hh_collections]
    cursor_0=cursors[0]
    cursors_rem=cursors[1:]
    Ts=[] 
    V=[]
    self.print_msg('Retrieved %d samples\n'%cursor_0.count())
    for i in range(0,cursor_0.count()):
      curr_elem=cursor_0.next() 
      Ts.append(curr_elem['index'])
      V.append(curr_elem['hh_load'] + reduce(lambda x,y: x+y, \
        [c.next()['hh_load'] for c in cursors_rem]))

    modified_Ts,modified_V= self.\
      curate_for_granularity(query['granularity'],Ts,V) 

    return modified_Ts,modified_V

  def curate_for_granularity(self,granularity,Ts,V):
    if (granularity==common.Granularity.win_5mins.name):
      return Ts,V
    else:
      modified_Ts=[]
      modified_V=[]
      granularity_value=common.Granularity[granularity].value
      num_elems=len(Ts)//granularity_value
      for i in range(0,num_elems*granularity_value,granularity_value):
        modified_Ts.append(Ts[i])
        modified_V.append(reduce(lambda x,y:x+y,\
          [V[idx] for idx in range(i,i+granularity_value)]))
      return modified_Ts,modified_V


  def outlier(self,query):
    self.print_msg('Received outlier query')
    return None

  def send_request(self,query_type,granularity,\
    query_level,query_target,prediction_win_ts,history):
    msg={'query_type': query_type.name,\
      'granularity': granularity.name,\
      'query_level': query_level.name,\
      'query_target': query_target,\
      'prediction_win_ts':prediction_win_ts,\
      'history': history.name,
      'use_initial_model':False}
    self.process(msg)


if __name__=="__main__":
  processor=Processor(verbose=True) 
  #previous_model=processor.get_previous_model_if_it_exists(common.QueryLevel.plug.name,
  #  "11_0_0",common.Granularity.win_5mins.name)
  #print(previous_model)
  rand_idx=random.randint(0,common.no_valid_query_readings)
  prediction_win_ts= common.start_query_ts + rand_idx*common.step_size
  processor.send_request(common.QueryType.load,\
    common.Granularity.win_60mins,\
    common.QueryLevel.plug,\
    '11_0_0',
    prediction_win_ts,
    common.History.past_5days)
