'''
Created on Aug 8, 2009

@author: eorbe
'''

import urllib
import os.path
import traceback
import os.path
from urllib import FancyURLopener
from random import choice

class BaseDownloader:
    __abstract__  = 'BaseCrawler is an abstract class!'
    
    def __init__(self,download_record,folder_path,logger,retries=1):
        self.download_record=download_record
        self._temp_filename=None
        self._logger=logger
        self.progress=0
        self.downloaded_size=0
        self.download_ok=True
        self.download_error=None
        self._last_progress_shown=0
        self.max_download_retries=retries
        self._folder_path=folder_path
        self._base_filename='video-'+download_record['video-id']
        
               
    def start(self):
        raise Exception(self.__abstract)
          
    def download(self,url,filename):
        try:
            retries=0
            self._temp_filename=filename
            self._logger.info('Starting the download of url: %s.',url)
            self._logger.debug('Local Filename to create: %s',filename)
            while retries<self.max_download_retries:
                retries=retries+1
                self._logger.info('Here we go...Attempt # %s to download....',retries)
                self._do_download(url,filename)
                if self.download_ok:
                    self._logger.info('Yeah!...Download of url %s ended successfully. File %s was created',url,filename)
                    break
                else:
                    self._logger.info('Don''t freak out...lets try again...')
            if not self.download_ok:
                self._logger.info('Sh*t!...I did my best, but we had no luck...Check the url and try again later.')
        except:
            self._logger.exception('WTF!!!...An error occurred while downloading from url %s.', url)
            self.download_ok=False
        finally:
            return self.download_ok
        
    def _do_download(self,url,filename):
        try:
            urllib.urlretrieve(url, filename, self._progress_callback)
        except:
            self._logger.exception('Dammit!..An error occurred while downloading from url %s.', url)
            self.download_error=traceback.format_exception_only(sys.last_type, sys.last_value)
            if os.path.exists(filename):
                os.remove(filename)
            self.download_ok=False
    
    def _build_result_record(self,url,quality):
        result= {'url':url,
                'filename':self._temp_filename,
                'quality': quality,
                'downloaded': self.download_ok,
                'download_error':self.download_error,}
        self._logger.debug('Download result: %s', result)
        return result
    
    def _get_file_name(self,quality):
        key='extension'+quality
        return os.path.join(self._folder_path,self._base_filename+'-'+quality+'.'+self.download_record[key])
             
    def _progress_callback(self,block_count,block_size,file_size):
        if file_size>-1:
            self.downloaded_size=block_count*block_size
            perc=(self.downloaded_size*100)/file_size
            if (perc % 10 ) == 0 and perc!=self._last_progress_shown:
                self._last_progress_shown=perc
                self._logger.info('Downloaded %.2f MB from %.2f MB - %s %%' % (((float(self.downloaded_size)/float(1024))/float(1024)),((float(file_size)/float(1024))/float(1024)),perc))
        else:
            os.remove(self._temp_filename)
            self.download_ok=False
            self.download_error='File not found.'
            self._logger.error('Ouch!!...File not found. Url: %s',self.url)




class CustomUrlOpener(FancyURLopener):
    _user_agents = [   'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
                        'Opera/9.25 (Windows NT 5.1; U; en)',
                        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
                        'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
                        'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
                        'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9'
                   ]
    version=choice(_user_agents)
    
        