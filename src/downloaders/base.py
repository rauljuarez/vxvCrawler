'''
Created on Jul 18, 2009

@author: eorbe
'''
import urllib
import logging
import sys
import os.path
import ftplib
import utils.distributed_logging
import cPickle
import zipfile
import traceback
import time
from xml.dom.minidom import Document
from multiprocessing import Pool, cpu_count,current_process

from downloaders.youtube_downloader import YouTubeDownloader
from downloaders.vimeo_downloader import VimeoDownloader
from downloaders.metacafe_downloader import MetacafeDownloader
from downloaders.megavideo_downloader import MegaVideoDownloader
from downloaders.dailymotion_downloader import DailyMotionDownloader
from utils.vxv_service import VXVService
try:
    from xml.etree import cElementTree as ElementTree
except ImportError, e:
    from xml.etree import ElementTree

logger=None
queue_logger=None

def _config_logging(queue=None):
    if queue!=None:
        return utils.distributed_logging.QueueLogger(queue,'downloaders.video_downloader')
    else:
        return logging.getLogger('downloaders.video_downloader')

def batch_poller(service_host, client_id,poll_time,queue_folder):
    _logger=logging.getLogger('batch_poller')
    _logger.info('Polling the service for a download batch...')
    srv=VXVService(service_host, client_id, 1)
    count,xml_data=srv.get_download_batch()
    if count>0:
        try:
            time.sleep(5)
            _logger.info('There is a download batch available, saving it to the disk...')
            file_name=os.path.join(queue_folder, ('download-batch-%s.xml' % int(time.time())))
            f=open(file_name,'w')
            f.write(xml_data)
            f.close()
        except:
            _logger.exception('An error occurred while saving the received download batch...Exiting...')
            sys.exit(1)
    else:
        _logger.info('There is no download batch available. Polling again in %s seconds..',poll_time)
    time.sleep(poll_time)
    batch_poller(service_host, client_id,poll_time,queue_folder)
    
    
_config=None
def batch_processor(config,task_queue,logging_queue=None):
   
    global logger
    global queue_logger
    queue_logger=_config_logging(logging_queue)
    logger=_config_logging(None) 
      
    if task_queue!=None:
        while True:
            #get the file from the pool
            batch_file_name=task_queue.get()
            if batch_file_name != None:
                try:    
                    base_file_name=os.path.splitext(batch_file_name)[0]
                    
                    logger.info('Starting the processing of batch file %s ...',batch_file_name)
            
                    dbatch=parse_xml_download_batch(os.path.join(config['queuePath'],batch_file_name))
                            
                    download_results=download_batch(dbatch, config)
                    dbatch_file=os.path.join(config['queuePath'],(base_file_name+'.dbpkl'))
                    cPickle.dump(download_results, open(dbatch_file,'wb'))
                    
                    upload_results=upload_batch(download_results, config)
                    ubatch_file=os.path.join(config['queuePath'],(base_file_name+'.ubpkl'))
                    cPickle.dump(upload_results, open(ubatch_file,'wb'))
            
                    results_xml=build_xml_results_batch(upload_results,config)
                    
                    send_ok=send_results_to_service(results_xml,base_file_name,config)
                    if not send_ok:
                        base_file_name=base_file_name+-'not-sended'
                    clean_up(base_file_name, config)
                    
                except:
                    logger.exception('An error occurred while processing batch file %s ....',batch_file_name)
                    sys.exit(0)
            else:
                time.sleep(10)    
    else:
        logger.error('You must provide a task queue if you intend to run the processor in a multithreading fashion.')
        sys.exit(0)
    
        

def send_results_to_service(results,file_name,config):
    try:
        xml_file_name=os.path.join(config['queuePath'],file_name+'-results.rxml')
        max_send_retries=config['max_send_results_retries']

        logger.info('Saving xml results file before sending it to the server. File:%s ...',xml_file_name)
        f=open(xml_file_name,'w')
        f.write(results)
        f.close()
        
        retries=1
        srv=VXVService(config['serviceHost'], config['client_id'],config['client_type'])

        while retries <= max_send_retries:
            logger.info('Sending results to the server. Attempt # %s..',retries)
            if srv.send_download_results(results):
                logger.info('The server received the results correctly...')
                return True
            logger.info('Something happened!!...The server didn''t get the results...trying again...')
            retries=retries+1
        logger.info('Dammit!!...The server didn''t get the results...moving on...')    
        return False
    except:
        logger.exception('Ouch!..An exception occurred while sending the results back to the server...')
    

def clean_up(batch_file_name,config):
    
    logger.info('Uuhgggg!, this is a messs...let''s make some cleanup...')

    #first we pack the processed batch and intermediate files and send them to the processed folder.
    #tarf=tarfile.open(os.path.join(config['processedPath'],(batch_file_name+".tar.gz")), "w:gz")
    tarf=zipfile.ZipFile(os.path.join(config['processedPath'],(batch_file_name+".zip")), "w")
    try:
        logger.info('Packing the generated files in the queue folder...')
        for ext in ['.xml','.dbpkl','.ubpkl']:
            fname=os.path.join(config['queuePath'],(batch_file_name+ext))
            tarf.write(fname)
        logger.info('FYI...You have all the files packed in the ''processed folder'' under the name of %s...',(batch_file_name+".zip"))    
    except:
        logger.exception('WTF!!!...An error occurred while packing the batch files...')
    else:
        try:    
            logger.info('Ok, the time of doom has arrived...let''s delete the unnecesary files...')
            if str.upper(config['deleteDownloadedFiles'].encode('utf-8'))=='TRUE':
                dbatch=cPickle.load(open(os.path.join(config['queuePath'],(batch_file_name+'.dbpkl')),'rb'))
                for item in dbatch:
                    for file in item['results']:
                        if file['downloaded']==True:
                            os.remove(file['filename'])
                            logger.info('Good...%s has been deleted...',file['filename'])
                            
            for ext in ['.xml','.dbpkl','.ubpkl']:
                fname=os.path.join(config['queuePath'],(batch_file_name+ext))
                os.remove(fname)
                logger.info('Good...%s has been deleted...',fname)
        except:
            logger.exception('Dammit!!!...An error occurred while deleting the unnecesary files...')        
    finally:
        tarf.close()
        
    logger.info('The cleanup is done...')   

def upload_batch(upload_batch,config):
    
    logger.info('Starting batch upload...')
    upload_batch_results=[]
    try:
        global _config
        _config=config
        
        process_count=_get_process_count(config)
        logger.info('Building upload workers pool with %s worker processes ...' % process_count)
        process_pool=Pool(processes=process_count)
        
        upload_batch_results=process_pool.map(_upload_video_async,upload_batch)
        #upload_batch_results=(_upload_video_async(upload_batch[0]),)
        process_pool.close()
        logger.debug('Upload results:%s ...',upload_batch_results)       
        logger.info('Batch upload ended successfully...')
        
        return upload_batch_results
    except:
        logger.exception('An error occurred while uploading batch')
        
def _upload_video_async(item):
    try:
        ftp=FtpFileUploader(_config['ftpServer'],_config['ftpUser'],_config['ftpPass'],_config['ftpFolder'],queue_logger,_config['ftpPort'],_config['max_upload_retries'])
        ftp.connect_to_ftp()
        for file in item['results']:
            if file['downloaded']==True:
                ftp.upload(file['filename'])
                file['uploaded']=ftp.upload_ok
                file['upload_error']=ftp.upload_error
            else:
                file['uploaded']=False
                file['upload_error']=None
        return item
    finally:   
         ftp.disconnect_from_ftp()
    
def build_xml_results_batch(notify_batch,config):
    
    doc=Document()
    root=doc.createElement('downloads')
    doc.appendChild(root)
    
    for item in notify_batch:
        notif=doc.createElement('download')
        notif.setAttribute('video-id',item['video-id'])
        for result in item['results']:
            video=doc.createElement('video')
     
            video.setAttribute('quality',result['quality'])
            #video.setAttribute('extension',result['extension'])
            video.setAttribute('downloaded',str(result['downloaded']))
            video.setAttribute('uploaded',str(result['uploaded']))
            if result['filename']!=None:
                video.setAttribute('filename',os.path.basename(result['filename']))
            else:
                video.setAttribute('filename','')
                
            url=doc.createElement('url')
            url.appendChild(doc.createTextNode(result['url']))
            video.appendChild(url)
            
            if result['upload_error']!=None:
                up_error=doc.createElement('up-error')
                up_error.appendChild(doc.createTextNode(result['upload_error']))
                video.appendChild(up_error)
            
            if result['download_error']!=None:
                down_error=doc.createElement('down-error')
                down_error.appendChild(doc.createTextNode(result['download_error']))
                video.appendChild(down_error)
            
            notif.appendChild(video)
            
        root.appendChild(notif)
    xml_str=doc.toxml()
    doc.unlink()
    return xml_str    


def build_service_notification(upload_result):
    return None



def download_batch(download_batch,config):
    global _config
    _config=config
    
    logger.info('Starting batch download...')
    process_count=_get_process_count(config)
    logger.info('Building download workers pool with %s worker processes ...' % process_count)
    process_pool=Pool(processes=process_count)
    
    batch_download_results=process_pool.map(_download_video_async,download_batch)
    #batch_download_results=(_download_video_async(download_batch[0]),)
    
    process_pool.close()
    logger.info('Batch download ended successfully...')
    logger.debug('Download results:%s ...',batch_download_results) 
      
    return batch_download_results

def _download_video_async(item):
   downloader=get_downloader(item['source'],item,_config['downloadsFolder'],_config['max_download_retries'])
   return {'video-id':item['video-id'],'results':downloader.start(),}

def get_downloader(source,download_record,folder_path,retries):
    if source=="1":
        return YouTubeDownloader(download_record,folder_path,queue_logger,retries)
    elif source=="2":
        return VimeoDownloader(download_record,folder_path,queue_logger,retries)
    elif source=="3":
        return MetacafeDownloader(download_record,folder_path,queue_logger,retries)
    elif source=="4":
        return MegaVideoDownloader(download_record,folder_path,queue_logger,retries)
    elif source=="5":
        return DailyMotionDownloader(download_record,folder_path,queue_logger,retries)
    else:
        queue_logger.error('No downloader founded...')
        return None

def _get_process_count(config):
    process_count=cpu_count()
    if config['maxCores']>=1 and config['maxCores']<=process_count:
        process_count=config['maxCores']
    return process_count

def parse_xml_download_batch(xml_file):
    #load the xml in memory
    tree=ElementTree.parse(xml_file)
    results = []
    for i, elem in enumerate(tree.getiterator('download')):
        urls=[]
        for url in elem.findall('url'):
            urls.append({'quality':url.get('quality'),
                         'url':url.text,})
        results.append(
               {'video-id': elem.get('video-id'),
                'source':elem.get('source'),
                'extension0':elem.get('extension0'),
                'extension1':elem.get('extension1'),
                'extension2':elem.get('extension2'),
                'urls': urls,})
    logger.debug('Parsed download batch: %s' % results)
    return results

class FtpFileUploader:

    def __init__(self,server,user,passw,folder,logger,port=None,retries=1):
        self._logger=logger
        self.server=server
        self.port=port if port!=None else 21
        self.user=user
        self.password=passw
        self.folder=folder
        self._ftp=None
        self.max_upload_retries=retries
        self._file_size=0
        self.upload_ok=True
        self.uploaded_size=0
        self._block_size=8192
        self._block_count=1
        self._last_progress_shown=0
        self.upload_error=None
    
    def _reset(self):
        self._file_size=0
        self.upload_ok=True
        self.uploaded_size=0
        self._block_size=8192
        self._block_count=1
        self._last_progress_shown=0
            
    def connect_to_ftp(self):
        try:
            self._ftp = ftplib.FTP()
    
            self._logger.info('Dude...we are connecting to ftp server %s at port %s ...',self.server,self.port)
            self._ftp.connect(self.server,self.port)
            
            self._logger.info('Now, trying to logging in to ftp server as user %s ...',self.user)
            self._ftp.login(self.user, self.password)
            self._logger.info('Great!...Logging succesfull...')
            
            if self.folder!='' and self.folder!=None:
                self._logger.info('Switching to designated directory %s', self.folder)
                self._ftp.cwd(self.folder)
        except:
            self._logger.exception('WTF!!!..An error occurred while connecting to the ftp server %s, port: %s.', self.server,self.port)
        
    def disconnect_from_ftp(self):
        try:
            self._logger.info('Ok, it seems that the job is done...disconnecting from ftp server...')
            if self._ftp!=None:
                self._ftp.quit()
            self._logger.info('Ciaooo!...the connection has been closed...')    
        except:
            self._logger.exception('Dammit!...An error occurred while disconnecting from the ftp server...')
            
        
    def upload(self,filename):
        try:
            retries=0
            self._reset()
            self._file_size=os.path.getsize(filename)
            fname = os.path.split(filename)[1]
            self._logger.info('Ok, lets try this...starting the upload of file %s to ftp server... ',filename)
            while retries<self.max_upload_retries:
                retries=retries+1
                self._logger.info('Here we go...Attempt # %s to upload....',retries)
                self._do_upload(filename,fname)
                if self.upload_ok:
                    self._logger.info('Sweet!!...Upload of file %s ended successfully...',filename)
                    break
                else:
                    self._logger.info('Don''t freak out...lets try again...')
            if not self.upload_ok:
                self._logger.info('Sh*t!...I did my best, but we had no luck...Check the if the server is up and we have the right permissions and try again later.')
        except:
            self._logger.exception('An error occurred while uploading file %s.',filename)
            self.upload_ok=False
        finally:
            return self.upload_ok
        
    def _do_upload(self,fullname,fname):
        try:
            try:
                f = open(fullname, "rb")
                self._ftp.storbinary('STOR ' + fname, f,self._block_size,self._progress_callback)
            finally:
                f.close()
        except:
            self._logger.exception('Dammit!!...An error occurred while uploading file %s.',fullname)
            self.upload_ok=False
            
    def _progress_callback(self,b):
        self.uploaded_size=self._block_count*self._block_size
        self._block_count=self._block_count+1
        perc=(self.uploaded_size*100)/self._file_size
        if (perc % 10 ) == 0 and perc!=self._last_progress_shown:
            self._last_progress_shown=perc
            self._logger.info('Uploaded %.2f MB from %.2f MB - %s %%' % (((float(self.uploaded_size)/float(1024))/float(1024)),((float(self._file_size)/float(1024))/float(1024)),perc))


    
            

            