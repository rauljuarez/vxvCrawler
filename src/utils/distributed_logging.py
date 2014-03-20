'''
Created on Jul 29, 2009

@author: eorbe
'''
import os,sys,threading,logging,traceback
from multiprocessing import current_process
LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

class QueueLoggingHandler(threading.Thread):

    def __init__(self,queue):
        threading.Thread.__init__(self)
        self._queue=queue
        self._logger=logging.getLogger('logging_thread')
    
    def run(self):
        while True:
            try:
                log_msg=self._queue.get()
                if log_msg!=None:
                    #new_msg='PID:'+log_msg.pid+' - remote_logger:'+log_msg.logger_name+' - '+log_msg.msg
                    new_msg='PID:%s  - %s' % (log_msg.pid,log_msg.msg)
                    self._logger.log(LEVELS[log_msg.level],new_msg,*log_msg.args,**log_msg.kwargs)
            except:
                self._logger.exception('An error ocurred while logging from a distributed process...')
                
                        
                
class QueueLoggingMessage:
  
    def __init__(self,level,msg,pid,logger_name,*args,**kwargs):
        self.msg=msg
        self.logger_name=logger_name
        self.pid=pid
        self.level=level
        self.args=args
        self.kwargs=kwargs

class QueueLogger:
    def __init__(self,queue,logger_name):
        #self._pid=current_process().pid
        self._logger_name=logger_name
        self._queue=queue
    
    def _put_in_queue(self,level,msg,*args,**kwargs):
        self._queue.put(QueueLoggingMessage(level,msg,current_process().pid,self._logger_name,*args,**kwargs))
            
    def debug(self,msg,*args):
        self._put_in_queue('debug',msg,*args)
    
    def info(self,msg,*args):
        self._put_in_queue('info',msg,*args)
    
    def exception(self,msg,*args):
        type,value,trace_back=sys.exc_info()
        try:
            if trace_back!=None:
                msg=msg + '\n' + traceback.format_exc()
                #msg=msg + '\n\n' + traceback.format_tb(trace_back)[0] +'\n Exception:' + str(type)        
            self._put_in_queue('error',msg,*args)
        finally:
            trace_back=None
    
    def error(self,msg,*args):        
        self._put_in_queue('error',msg,*args)

    def critical(self,msg,*args):        
        self._put_in_queue('critical',msg,*args)
        
    def warning(self,msg,*args):
        self._put_in_queue('warning',msg,*args)