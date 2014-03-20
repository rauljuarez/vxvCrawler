'''
Created on Aug 11, 2009

@author: eorbe
'''

from utils.BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
from downloaders.base_downloader import BaseDownloader,CustomUrlOpener
import urllib
import re

from xml.etree import cElementTree as ElementTree


class MetacafeDownloader(BaseDownloader):
    
    def __init(self,download_record,folder_path,logger,retries=1):
        BaseDownloader.__init__(self, download_record,folder_path,logger,retries)

    def start(self):
        """Starts the downloading of the different qualities of a video. Returns a list with the results."""
        url=self.download_record['urls'][0]['url']
        lq_url=self._get_video_info(url)
        
        results=[]
        
        if lq_url!=None:
            self._logger.info('Let''s download the low quality version of the video at url %s...',lq_url)
            self.download(lq_url, self._get_file_name('0'))
        else:
            self.download_ok=False
            self._logger.info('The metacafe video url couldn''t be found. Maybe Metacafe changed their html. The page url is %s',url)
            self.download_error='The video url couldn''t be found. Maybe Metacafe changed their html.'

        results.append(self._build_result_record(lq_url,'0')) 
        
        return results
    
       
    def _get_video_info(self,video_url):  
        ''' 
        Return direct URL to video.
        '''  
        #get the page
        data=urllib.urlopen(video_url)
        soup = BeautifulSoup(''.join(data.read()),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
        #find the location of the embed code
        div=soup.find('noscript')
        if div!=None:
            rex= re.compile(r'mediaURL=(.*?)&', re.M)
            flashvars=div.contents[1].attrs[9][1].encode('utf-8')
            self._logger.debug('Metacafe flashvars:%s',flashvars)
            match=rex.search(flashvars)
            if match!=None:
                return urllib.unquote(match.group(1))
            else:
                return None
        else:
            return None
