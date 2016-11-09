from common import State, WinSizes,perf_metrics, house_forecasts
from median import StreamingMedian

class PlugModel(object):
  def __init__(self,h_id,hh_id,plug_id):
    self.h_id=h_id
    self.hh_id=hh_id
    self.plug_id=plug_id
    self.last_ts=-1
    self.update_window= State(last_ts= -1,
      load=0.0,
      count=0)
    self.time_windows= { size.name: State(last_ts=-1, load=0.0, count=0) \
      for size in WinSizes}
    self.median_containers= { size.name: {} for size in WinSizes } 
    self.initialized=False
    self.perf=perf_metrics[self.h_id]
    self.result_file=house_forecasts[self.h_id]
    
  
  def process_update(self,update):
    #cache current reading update
    reading=update.reading

    # Initialization
    if (not self.initialized):
      self.last_ts=update.last_ts    
      for key,value in self.time_windows.items():
        value.last_ts=reading.ts - \
          (WinSizes[key].value if (reading.ts%WinSizes[key].value==0) \
            else (reading.ts%WinSizes[key].value))
      self.initialized= True
    
    #30 sec global time window for triggering updates has elapsed.
    if (self.last_ts != update.last_ts):
      self.last_ts=update.last_ts
      #process time update
      self.process_time_update(reading)
    
    #process reading update
    self.process_reading(reading)
   
    #record perf metrics
    self.perf.record(update.reception_ts)
    

  def process_time_update(self,reading):
    #if plug update is in sync with global time update, then do nothing
    if (self.update_window.last_ts == self.last_ts):
      return

    for win_size, state in self.time_windows.items():
      #global time has progressed past current window 
      if ((self.last_ts - state.last_ts) > WinSizes[win_size].value):
        state.load=0
        state.count=0
        state.last_ts= self.last_ts - (self.last_ts % WinSizes[win_size].value)
        continue
      
      #publish load forecast
      self.forecast(reading,self.last_ts,win_size)
    
    # global time has progressed, clear update_window
    self.update_window.load= 0
    self.update_window.count= 0
    self.update_window.last_ts= self.last_ts
   
  def process_reading(self,reading):
    #global update window has not elapsed
    if ((reading.ts - self.update_window.last_ts) <= WinSizes.win_30s.value):
      self.update_window.load+= reading.value
      self.update_window.count+= 1

    #global update window has elapsed and load forecasts need to be published
    if ((reading.ts- self.update_window.last_ts) == WinSizes.win_30s.value):
      for win_size in self.time_windows.keys():
        self.forecast(reading,reading.ts,win_size) 
      self.update_window.load=0
      self.update_window.count=0
      self.update_window.last_ts=reading.ts

    if ((reading.ts -self.update_window.last_ts) > WinSizes.win_30s.value):
      self.update_window.load= reading.value
      self.update_window.count= 1
      self.update_window.last_ts= reading.ts - \
       (WinSizes.win_30s.value if ((reading.ts%WinSizes.win_30s.value)==0) else \
       (reading.ts % WinSizes.win_30s.value))


  def forecast(self,reading,forecast_ts,win_size):
    state= self.time_windows[win_size]

    state.load+= self.update_window.load
    state.count+= self.update_window.count

    avg_load=0.0
    if (state.count > 0):
      avg_load= state.load/state.count        
      self.forecast_plugload(forecast_ts,avg_load,win_size)

    if(forecast_ts - state.last_ts == WinSizes[win_size].value): 
      #add avg_load to median container
      median_container= self.median_containers[win_size]
      if (state.last_ts%86400) in median_container:
        median_container[state.last_ts%86400].insert(avg_load)
      else:
        median_container[state.last_ts%86400]= StreamingMedian()
      state.load=0
      state.count=0
      state.last_ts=forecast_ts

  def forecast_plugload(self,forecast_ts,avg_load,win_size):
    prediction_window_ts= forecast_ts - \
      (WinSizes[win_size].value if (forecast_ts%WinSizes[win_size].value==0) else \
      (forecast_ts%WinSizes[win_size].value)) + 2*WinSizes[win_size].value
    median_container= self.median_containers[win_size]
    median=0.0
    if (forecast_ts%86400) in median_container:
      median= median_container[forecast_ts%86400].get_median()
    
    forecast= (avg_load + median)/2
    self.result_file.write('%s,%d,%d,%d,%d,%f\n'% \
      (win_size,prediction_window_ts,self.h_id,self.hh_id,self.plug_id,forecast))
    self.result_file.flush()
    #print('load forecast h_id:%d hh_id:%d, plug_id:%d, \
    #  win_size:%s, prediction win_ts:%d, forecast:%f'%\
    #  (self.h_id,self.hh_id,self.plug_id,win_size,prediction_window_ts,forecast))
