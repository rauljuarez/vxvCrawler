import os, time, logging,threading

class DirectoryMonitor:
    
    def __init__(self,queuePath,pollingTime,callback,logging_queue=None,ignore_ext=None,processExisting=False):
        #threading.Thread.__init__(self)
        self._logging_queue=logging_queue
        self.logger=logging.getLogger('utils.filemonitor')
        self.path_to_watch=queuePath
        self._callback=callback
        self.batch_size= 1 #if not fillbatchsize else conf.batchSize
        self.processExistingFiles=processExisting
        self.polling_time=pollingTime
        self.ignored_extensions=ignore_ext
    
    def run(self):
        self.monitor()
        
    def monitor(self): 
        self.logger.info('Monitoring directory: %s ...' % self.path_to_watch)
        before = {} if self.processExistingFiles else dict ([(f, None) for f in os.listdir (self.path_to_watch) if not os.path.isdir(os.path.join(self.path_to_watch,f))])
        added = []
        while 1:
            time.sleep(self.polling_time)
            after = dict ([(f, None) for f in os.listdir (self.path_to_watch) if not os.path.isdir(os.path.join(self.path_to_watch,f)) and not os.path.splitext(f)[1] in self.ignored_extensions])
            
            if self.batch_size==1:
                added = [f for f in after if not f in before]
                if added: 
                    self.logger.info('Files added to the processing queue: %s ...', ", ".join (added))
                    
                    if self._logging_queue!=None:
                        #call the callback function
                        self._callback(added,self._logging_queue)
                    else:
                        self._callback(added)
                        
            else:
                temp= added + [f for f in after if not f in before]
                
                if len(temp)>= self.batch_size:
                    temp1=temp[0:self.batch_size]
                    self.logger.info('Files added to the processing queue: %s ...', ", ".join (temp1))
                    #call the callback function
                    if self._logging_queue!=None:
                        #call the callback function
                        self._callback(temp1,self._logging_queue)
                    else:
                        self._callback(temp1)

                    added=temp[self.batch_size:]
                else:
                    added=temp
                    
            before = after