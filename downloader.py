'''
Created on Jul 16, 2009

@author: eorbe
'''
#!/usr/bin/python
import os.path
import sys
import logging
import logging.config
import threading
import Queue
from multiprocessing import Pool, cpu_count, freeze_support,Manager

sys.path.append(os.path.join(os.path.dirname(sys.argv[0]),'src'))
from utils.configuration import LoadXmlConfigurationFromFile,LoadAndMergeRemoteConfiguration,load_pass,save_pass
from utils.filemonitor import DirectoryMonitor
import utils.distributed_logging
import downloaders.base   


logger=None
process_pool=None
config=None
task_queue=Queue.Queue(10)

def assign_task(tasks):
    
    for task in tasks:
        logger.debug('Adding new task to the queue. Batch to process: %s' % task)
        task_queue.put(task, True)
        #downloaders.base.process_batch(task,config,None)
        #process_pool.apply(downloaders.video_downloader.ProcessBatch,(task,config,logging_queue))

def Main():
    try:
        config_file=os.path.join(os.path.dirname(sys.argv[0]),'downloader','d_config.xml')
        pass_file=os.path.join(os.path.dirname(sys.argv[0]),'downloader','.data')
        
        if os.path.exists(pass_file):
            client_id=load_pass(pass_file)
        else:
            client_id=0
        client_type=1
        
        #load the configuration        
        print 'Loading local configuration...'
        
        global config
        config=LoadXmlConfigurationFromFile(config_file)
        
        #build log config filename.
        clogname=os.path.join(os.path.dirname(sys.argv[0]),'downloader',config['logConfigFile'])
        
        #load logging configuration
        logging.config.fileConfig(clogname)
        
        # create logger
        global logger 
        logger = logging.getLogger("downloader")
        
        logger.info('Local configuration loaded successfully...')
        
        logger.info('Loading remote configuration...')
        
        srv_av,new_id,config=LoadAndMergeRemoteConfiguration(config, config['serviceHost'],client_id,client_type)
        
        if not srv_av:
            logger.info('Ouch!...The service is not available, try later...goodbye!...')
            sys.exit(0)
        
        if new_id!=client_id:
            client_id=new_id
            save_pass(pass_file,client_id)
        
        logger.info('Remote configuration loaded successfully...')
        config['client_id']=client_id
        config['client_type']=client_type
    
        man=Manager()
        logging_queue=man.Queue()
        
        #create the processors threads
        logger.info('Starting batch processor thread...')
        th=threading.Thread(target=downloaders.base.batch_processor,args=(config, task_queue,logging_queue,))
        th.start()
        
      
         #start the logging thread
        logger.info('Starting logging monitor...')
        log_thread= utils.distributed_logging.QueueLoggingHandler(logging_queue)
        log_thread.start()
        
        #start the service polling thread
        logger.info('Starting service poller...')
        threading.Thread(target= downloaders.base.batch_poller,args=(config['serviceHost'], client_id,config['download_service_polling'],config['queuePath'],)).start()  
    
        #start the directory monitor
        logger.info('Starting queue monitor...')
        ignored_extension=['.dbpkl','.ubpkl','.ntpkl','.rxml']
        DirectoryMonitor(os.path.dirname(config['queuePath']),config['queuePolling'],assign_task,ignore_ext=ignored_extension).monitor()
    except IOError:
        print '\nBye...'
        sys.exit(0)
    except KeyboardInterrupt:
        print '\nBye...'
        sys.exit(0)
  
    
if __name__ == '__main__':
    freeze_support()
    Main()
    