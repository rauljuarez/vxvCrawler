#!/usr/bin/python
import os.path
import sys
import logging
import logging.config
import threading
import Queue
import threading

sys.path.append(os.path.join(os.path.dirname(sys.argv[0]),'src'))
from utils.configuration import LoadXmlConfigurationFromFile,LoadAndMergeRemoteConfiguration,load_pass,save_pass
from utils.filemonitor import DirectoryMonitor
import crawlers.base   

logger=None
config=None
task_queue=Queue.Queue(10)

def assign_task(tasks):
    for task in tasks:
        logger.debug('Adding new task to the queue. Batch to process: %s' % task)
        task_queue.put(task, True)
        
def Main():
    try:
        config_file=os.path.join(os.path.dirname(sys.argv[0]),'crawler','c_config.xml')
        pass_file=os.path.join(os.path.dirname(sys.argv[0]),'crawler','.data')
        if os.path.exists(pass_file):
            client_id=load_pass(pass_file)
        else:
            client_id=0
        
        client_type=0
        #load the configuration        
        print 'Loading local configuration...'
        
        global config
        config=LoadXmlConfigurationFromFile(config_file)
        
        #build log config filename.
        clogname=os.path.join(os.path.dirname(sys.argv[0]),'crawler',config['logConfigFile'])
        
        #load logging configuration
        logging.config.fileConfig(clogname)
        
        # create logger
        global logger 
        logger = logging.getLogger("crawler")
        
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
    
        
        #create the processors threads
        logger.info('Starting batch processor threads...')
        for x in xrange(config['maxThreads']):
            th=threading.Thread(target=crawlers.base.batch_processor,args=(config, task_queue,))
            th.start()
    
        #start the service polling thread
        logger.info('Starting service poller...')
        threading.Thread(target= crawlers.base.batch_poller,args=(config['serviceHost'], client_id,config['search_service_polling'],config['queuePath'],)).start()  
       
    #      
        #start the directory monitor
        logger.info('Starting queue monitor...')
        ignored_extension=['.rxml']
        DirectoryMonitor(os.path.dirname(config['queuePath']),config['queuePolling'],assign_task,ignore_ext=ignored_extension).monitor()
    except KeyboardInterrupt:
        print '\nBye...'
        sys.exit(0)
        

if __name__ == '__main__':
    Main()
    
    