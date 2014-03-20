'''
Created on Sep 3, 2009

@author: eorbe
'''
from utils.BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
from downloaders.base_downloader import BaseDownloader,CustomUrlOpener
import urllib
import re

class DailyMotionDownloader(BaseDownloader):
    
    def __init(self,download_record,folder_path,logger,retries=1):
        BaseDownloader.__init__(self, download_record,folder_path,logger,retries)
        

    def start(self):
        """Starts the downloading of the different qualities of a video. Returns a list with the results."""
        url=self.download_record['urls'][0]['url']
        lq_url,hq_url,hd_url=self._get_video_info(url)
        
        results=[]
        
        if lq_url!=None:
            self._logger.info('Let''s download the low quality version of the video at url %s...',lq_url)
            self.download(lq_url, self._get_file_name('0'))
            results.append(self._build_result_record(lq_url, '0')) 

        if hq_url!=None:
            self._logger.info('Let''s download the high quality version of the video at url %s...',hq_url)
            self.download(hq_url, self._get_file_name('1'))
            results.append(self._build_result_record(hq_url, '1'))
        
        if hd_url!=None:
            self._logger.info('Let''s download the high definition version of the video at url %s...',hd_url)
            self.download(hd_url, self._get_file_name('2'))
            results.append(self._build_result_record(hd_url, '2')) 

        if lq_url==None and hq_url==None and hd_url==None:
            self.download_ok=False
            self._logger.info('None of the DailyMotion video urls could be found. Maybe DailyMotion changed their html or protocol. The page url is %s',url)
            self.download_error='The videos url couldn''t be found. Maybe DailyMotion changed their html or protocol.'
        
        
        return results
    
    def _get_video_id(self,url):
        b,s,a=url.rpartition('/')
        return a
    
    def _get_video_info(self,video_url):
         #class="dm_widget_videoplayer"
         
        opener=CustomUrlOpener()
        page=opener.open(video_url)
        response=page.read()
        page.close()
        
        soup = BeautifulSoup(''.join(response),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
        
        div=soup.find('div',{'class':'dm_widget_videoplayer'})
        script=div.find('script')
        if script!=None:
            urls= re.compile('addVariable\("video", "(.*?)"\);').findall(script.string)
            if urls!=None and len(urls)>0:
                return self._split_urls(urls[0])
            else:
                return None
        else:
            self._logger.error('We couldn''t get the dailymotion url of this video: %s',video_id)
            return None
    
    def _split_urls(self,raw_url):
        uq_urls=urllib.unquote(raw_url)
        urls=uq_urls.split('||')
        lq_url=None
        hq_url=None
        hd_url=None
        for url in urls:
            u,s,q=url.partition('@@')
            if q=='spark':
                lq_url=u
            elif q=='vp6-hq':
                hq_url=u
            elif q=='h264-hq':
                hd_url=u
        return lq_url,hq_url,hd_url
        
        