'''
Created on Aug 10, 2009

@author: eorbe
'''
from downloaders.base_downloader import BaseDownloader,CustomUrlOpener
import httplib
import urllib
from xml.etree import cElementTree as ElementTree

class VimeoDownloader(BaseDownloader):
    
    def __init(self,download_record,folder_path,logger,retries=1):
        BaseDownloader.__init__(self, download_record,folder_path,logger,retries)
        self._old_urlretrieve=urllib.urlretrieve
        opener=CustomUrlOpener()
        urllib.urlretrieve=opener.retrieve
    
    def start(self):
        """Starts the downloading of the different qualities of a video. Returns a list with the results."""
        url=self.download_record['urls'][0]['url']
        vid=self._get_video_id(url)

        lq_url,hd_url=self._get_video_info(vid)
        
        results=[]
        
        if lq_url!=None:
            self._logger.info('Let''s download the low quality version of the video at url %s...',lq_url)
            self.download(lq_url, self._get_file_name('0'))
            results.append(self._build_result_record(lq_url, '0'))
        
        if hd_url!=None:
            self._logger.info('Let''s download the high definition version of the video at url %s...',hd_url)
            self.download(hd_url, self._get_file_name('2'))
            results.append(self._build_result_record(hd_url, '2'))        
        
        if len(results)==0:
            self.download_ok=False
            self._logger.info('None of the Vimeo video urls could be found. Maybe Vimeo changed their html or protocol. The page url is %s',url)
            self.download_error='The videos url couldn''t be found. Maybe Vimeo changed their html or protocol.'

        #restore the function reference to the old urlretrieve
        #urllib.urlretrieve=self._old_urlretrieve
            
        return results
    
    def _get_video_id(self,url):
        b,s,a=url.rpartition('/')
        if b!=None:
            return a
        else:
            return None
    
    def _get_video_info(self,videoID):  
        ''' 
        Return direct URL to video and dictionary containing additional info 
        >> url,info = GetYoutubeVideoInfo("tmFbteHdiSw") 
        >> 
        '''  
        #httplib.HTTPConnection.debuglevel = 1
        conn = httplib.HTTPConnection("www.vimeo.com")
        conn.request('GET',('/moogaloop/load/clip:%s'%videoID))  
        response=conn.getresponse()
        signature,expire,has_hd=self._extract_response_parameters(response.read())
        
        #get the lq url
        conn.request('GET',('/moogaloop/play/clip:%s/%s/%s/' % (videoID,signature,expire)))
        response=conn.getresponse()
        lq_url=response.getheader('location')

        hd_url=None
        if has_hd=='1':
            conn.request('GET',('/moogaloop/play/clip:%s/%s/%s/?q=hd' % (videoID,signature,expire)))
            response=conn.getresponse()
            hd_url=response.getheader('location')
        
        return lq_url,hd_url
        
    def _extract_response_parameters(self,response_data):
        """ Extracts the required parameters from the body of the response, which is an xml string."""
        tree=ElementTree.fromstring(response_data)
        signature=tree.find('request_signature')
        expire=tree.find('request_signature_expires')
        has_hd=tree.find('video/isHD')
        
        return signature.text,expire.text,has_hd.text
        
  