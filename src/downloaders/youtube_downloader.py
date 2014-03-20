'''
Created on Aug 8, 2009

@author: eorbe
'''

from downloaders.base_downloader import BaseDownloader
import httplib
import urllib

class YouTubeDownloader(BaseDownloader):
    
    def __init(self,download_record,folder_path,logger,retries=1):
        BaseDownloader.__init__(self, download_record,folder_path,logger,retries)
        
    def start(self):
        """Starts the downloading of the different qualities of a video. Returns a list with the results."""
        url=self.download_record['urls'][0]['url']
        vid=self._get_video_id(url)

        lq_url,video_info=self._get_video_info(vid)
        
        hq_url,hd_url=self._extract_urls(video_info)
        
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
        
        if len(results)==0:
            self.download_ok=False
            self._logger.info('None of the youtube video urls could be found. Maybe YouTube changed their html or protocol. The page url is %s',url)
            self.download_error='The videos url couldn''t be found. Maybe YouTube changed their html or protocol.'
            
        return results
    
    def _get_video_id(self,url):
        b,s,a=url.partition('=')
        return a
    
    def _get_video_info(self,videoID,eurl=None):  
        ''' 
        Return direct URL to video and dictionary containing additional info 
        >> url,info = GetYoutubeVideoInfo("tmFbteHdiSw") 
        >> 
        '''  
        if not eurl:  
            params = urllib.urlencode({'video_id':videoID})  
        else :  
            params = urllib.urlencode({'video_id':videoID, 'eurl':eurl})
              
        conn = httplib.HTTPConnection("www.youtube.com")  
        conn.request("GET","/get_video_info?&%s"%params)  
        response = conn.getresponse()  
        data = response.read()  
        video_info = dict((k,urllib.unquote_plus(v)) for k,v in  
                                   (nvp.split('=') for nvp in data.split('&')))  
        conn.request('GET','/get_video?video_id=%s&t=%s' %  
                         ( video_info['video_id'],video_info['token']))  
        response = conn.getresponse()  
        direct_url = response.getheader('location')  
        return direct_url,video_info
    
    def _extract_urls(self,video_info):
        url_map=video_info.get('fmt_url_map',None)
        self._logger.debug('fmt_url_map: %s',url_map)
        if url_map!=None:
            url_list=url_map.split(',')
            filtered_url=[u.partition('|') for u in url_list if u.startswith('18') or u.startswith('22')]
            
            hq_url=None
            hd_url=None
            for url in filtered_url:
                if url[0]=='18':
                    hq_url=url[2]
                elif url[0]=='22':
                    hd_url=url[2]
            return hq_url,hd_url
        else:
            return None,None   
                
            
        