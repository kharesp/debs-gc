import common,time,random
from pymongo import MongoClient
from functools import reduce
from LoadModel_V1 import LoadModel

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
   
    hyp=[]
    if query['use_initial_model']: 
      hyp= self.get_previous_model_if_it_exists(query['query_level'],
        query['query_target'],
        query['granularity'])
      if not hyp:
        self.print_msg("Initial hyperparameters not found. will train initial model")
        hyp=self.train_initial_model(query)
      else:
        self.print_msg("Will use initial hyperparams:%s\n"%str(hyp))
    

    prediction_window_ts=query['prediction_win_ts']
    self.print_msg("prediction window ts:%d\n"%prediction_window_ts)

    history=query['history']
    start_ts=-1
    end_ts=-1
    if (history== common.History.past_1day.name):
      end_ts=prediction_window_ts-common.step_size
      start_ts=end_ts-(1*common.number_of_steps_in_a_day*\
        common.step_size)
      self.print_msg("calculated start_ts:%d and end_ts:%d\n"%(start_ts,end_ts))
    elif (history== common.History.past_3days.name):
      end_ts=prediction_window_ts-common.step_size
      start_ts=end_ts-(3*common.number_of_steps_in_a_day*\
        common.step_size + 2*common.step_size)
    elif (history== common.History.past_5days.name):
      end_ts=prediction_window_ts-common.step_size
      start_ts=end_ts-(5*common.number_of_steps_in_a_day*\
        common.step_size + 3*common.step_size)
    else:
      self.print_msg("history length:%s not recognized\n"%(history))
      return None

    self.print_msg("Will train on data with start_ts:%d and end_ts:%d\n"%\
      (start_ts,end_ts))
    gp_model=None
    prev_load=-1
    if(query['query_level']==common.QueryLevel.plug.name):
      hyperparams,gp_model,prev_load=\
        self.plug_level_load_prediction(query,start_ts,end_ts,hyp)
      self.print_msg('Finished training model. Hyperparameters:%s\n'%str(hyperparams))
      self.print_msg('prev_load:%f\n'%prev_load)
    elif (query['query_level']==common.QueryLevel.household.name):
      hyperparams,gp_model,prev_load=\
        self.household_level_load_prediction(query,start_ts,end_ts,hyp)
      self.print_msg('Finished training model. Hyperparameters:%s\n'%str(hyperparams))
      self.print_msg('prev_load:%f\n'%prev_load)
    elif (query['query_level']==common.QueryLevel.house.name):
      hyperparams,gp_model,prev_load=\
        self.house_level_load_prediction(query,start_ts,end_ts,hyp)
      self.print_msg('Finished training model. Hyperparameters:%s\n'%str(hyperparams))
      self.print_msg('prev_load:%f\n'%prev_load)
    else:
      self.print_msg('invalid query level')
      return None
   
    #prediction
    t_struct=time.gmtime(prediction_window_ts)
    index=(t_struct.tm_hour*60*60+\
      t_struct.tm_min*60+\
      t_struct.tm_sec)//common.step_size
    X=[prev_load,t_struct.tm_wday,index]
    mean,std= gp_model.predict_load(X)
    end_time=time.time()
    elapsed_time=end_time-start_time
    
    self.print_msg("Load Prediction results:\n")
    self.print_msg("Predicted mean:%f\nstd_dev:%f\nelapsed_time:%f seconds\n"%\
      (mean,std,elapsed_time))
    return {'mean':mean,\
      'sd':std,\
      'elapsed_time':elapsed_time}
    
  def get_previous_model_if_it_exists(self,query_level,
    query_target,query_granularity):
    document=self.client['model'][query_level].find_one({"_id":query_target})
    if document:
      if query_granularity in document.keys():
        return document[query_granularity]
      else:
        return None
    else:
      return None 

  def train_initial_model(self,query):
    hyp =None
    if(query['query_level']==common.QueryLevel.plug.name):
      hyp,gp_model,prev_load=self.plug_level_load_prediction(query,
        common.one_week_start_ts,common.one_week_end_ts)
    elif (query['query_level']==common.QueryLevel.household.name):
      hyp,gp_model,prev_load=self.household_level_load_prediction(query,
        common.one_week_start_ts,common.one_week_end_ts)
    elif (query['query_level']==common.QueryLevel.house.name):
      hyp,gp_model,prev_load=self.house_level_load_prediction(query,
        common.one_week_start_ts,common.one_week_end_ts)
    else:
      self.print_msg('invalid query level')
      return None
    
    self.client['model'][query['query_level']].\
      update_one({"_id":query['query_target']},\
      {"$set":{query['granularity']:hyp}},\
      upsert=True)

    return hyp 
    
  def plug_level_load_prediction(self,query,
    start_ts,end_ts,initial_model=[]):
    self.print_msg("plug_level_load_prediction called")
    target_plug=query['query_target']
    plug_id,hh_id,h_id=target_plug.split('_')
    cursor=self.client['load_%s'%h_id]['%s_%s'%(hh_id,h_id)].\
      find({"$and": [{"ts": {"$gt": start_ts-common.step_size}},\
      {"ts":{"$lt":end_ts+common.step_size}}]})
    self.print_msg('Retrieved %d samples\n'%cursor.count())
    DT=[]
    V=[]
    X=[]
    for reading in cursor:
      DT.append([reading['weekday'],reading['index']])
      V.append(reading['plugs'][target_plug])

    modified_DT,modified_V= self.\
      curate_for_granularity(query['granularity'],DT,V) 

    [X.append([modified_V[i-1]] + modified_DT[i]) \
      for i in range(1,len(modified_DT))]
    Y=modified_V[1:]
    hyp, gp_model=self.train(X,Y,initial_model)
    return hyp,gp_model,Y[-1]

  def household_level_load_prediction(self,query,
    start_ts,end_ts,initial_model=[]):
    target_hh=query['query_target']
    hh_id,h_id=target_hh.split('_')
    cursor=self.client['load_%s'%h_id][target_hh].\
      find({"$and": [{"ts": {"$gt": start_ts-common.step_size}},\
      {"ts":{"$lt":end_ts+common.step_size}}]})
    self.print_msg('Retrieved %d samples\n'%cursor.count())
    DT=[]
    V=[]
    X=[]
    for reading in cursor:
      DT.append([reading['weekday'],reading['index']])
      V.append(reading['hh_load'])

    modified_DT,modified_V= self.\
      curate_for_granularity(query['granularity'],DT,V) 

    [X.append([modified_V[i-1]] + modified_DT[i]) \
      for i in range(1,len(modified_DT))]
    Y=modified_V[1:]
    hyp,gp_model=self.train(X,Y,initial_model)
    return hyp,gp_model,Y[-1]

  def house_level_load_prediction(self,query,
    start_ts,end_ts,initial_model=[]):
    target_house=query['query_target']
    hh_collections=self.client['load_%s'%target_house].collection_names()
    cursors=[self.client['load_%s'%target_house][col].\
      find({"$and": [{"ts": {"$gt": start_ts-common.step_size}},\
      {"ts":{"$lt":end_ts+common.step_size}}]}) \
      for col in hh_collections]
    cursor_0=cursors[0]
    cursors_rem=cursors[1:]
    DT=[] 
    V=[]
    X=[]
    self.print_msg('Retrieved %d samples\n'%cursor_0.count())
    for i in range(0,cursor_0.count()):
      curr_elem=cursor_0.next() 
      DT.append([curr_elem['weekday'],curr_elem['index']])
      V.append(curr_elem['hh_load'] + reduce(lambda x,y: x+y, \
        [c.next()['hh_load'] for c in cursors_rem]))

    modified_DT,modified_V= self.\
      curate_for_granularity(query['granularity'],DT,V) 

    [X.append([modified_V[i-1]] + modified_DT[i]) \
      for i in range(1,len(modified_DT))]
    Y=modified_V[1:]
    hyp,gp_model=self.train(X,Y,initial_model)
    return hyp,gp_model,Y[-1]

  def curate_for_granularity(self,granularity,DT,V):
    if (granularity==common.Granularity.win_5mins.name):
      return DT,V
    else:
      modified_DT=[]
      modified_V=[]
      granularity_value=common.Granularity[granularity].value
      num_elems=len(DT)//granularity_value
      for i in range(0,num_elems*granularity_value,granularity_value):
        modified_DT.append(DT[i])
        modified_V.append(reduce(lambda x,y:x+y,\
          [V[idx] for idx in range(i,i+granularity_value)]))
      return modified_DT,modified_V

  def train(self,X,Y,hyperparams=[]):
    gp_model=None
    if hyperparams==[]:
      self.print_msg("training without hyperparams")
      gp_model=LoadModel(X,Y)
    else:
      self.print_msg("training with hyperparams:%s"%str(hyperparams))
      gp_model=LoadModel(X,Y,hyp=hyperparams)

    hyperparams=gp_model.train_model()
    return hyperparams,gp_model

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
  processor=Processor() 
  #previous_model=processor.get_previous_model_if_it_exists(common.QueryLevel.plug.name,
  #  "11_0_0",common.Granularity.win_5mins.name)
  #print(previous_model)
  rand_idx=random.randint(0,common.no_valid_query_readings)
  prediction_win_ts= common.start_query_ts + rand_idx*common.step_size
  processor.send_request(common.QueryType.load,\
    common.Granularity.win_5mins,\
    common.QueryLevel.plug,\
    '0_0_0',
    prediction_win_ts,
    common.History.past_5days)
