'''
Created on Aug 30, 2009

@author: eorbe
'''
import logging
import httplib
import urllib
import re
from random import choice
from xml.etree import cElementTree as ElementTree
from utils.BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
from crawlers.base_crawler import BaseCrawler,CustomUrlOpener
from utils.funcs import strip_accents

class MegaVideoCrawler(BaseCrawler):
    #http://www.megavideo.com/?c=search&s=
    def __init__(self,logger,search_id,search_max_results):
        BaseCrawler.__init__(self,logger,search_id,search_max_results)
    
    def search(self,search_terms,order_by='relevance'):
        """ Performs a search in MegaVideo an return a dictionary with the relevant metadata."""
        results=self._do_search(search_terms, order_by)
        if results!=None:
            return self._parse_feed(results)
        else:
            return []
    
    def get_video_metadata(self,vid,url):
        """ Retrieves the metada from a video located at url passed as an argument"""
        try:
            self._logger.info('Ok!...Lets try to retrieve some metada from MegaVideo')
            id=_get_video_id(url)
            if id!='':
                srv = MegaVideoService(self._logger)
                item_meta=self._parse_entry(srv.get_video_entry(id))
                item_meta['video-id']=str(vid)
                return item_meta
            else:
                self._logger.error('Ouch!...An illegal url was provided. It was impossible to get the video id.')
                return None
        except:
            self._logger.exception('Dammit!...An error ocurred while retrieving metadata from MegaVideo...')
            return None
        else:
            self._logger.info('Great!...The MegaVideo search was succesfull...')
            
    def _parse_feed(self,feed):
        """ Collects the relevant metadata from each entry of the search results feed.""" 
        meta=[]
        for entry in feed:
            item_meta=self._parse_entry(entry)
            item_meta['video-id']='0'
            meta.append(item_meta)
        self._logger.info('%s videos were founded and parsed at Megavideo',len(meta))    
        return meta

    def _parse_entry(self,entry):
        """ Collects the relevant metadata from a search result entry."""
        item_meta={'title':entry.title,
                   'description':entry.description,
                   'category':entry.category,
                   'tags':entry.tags,
                   'page_url':entry.url,
                   'lq_url':None,
                   'hq_url':None,
                   'hd_url':None,
                   'search-id':self.search_id,
                   'source':'4',}
        self._logger.debug('Video Metadata: %s',item_meta)
        return item_meta
   
    def _do_search(self,search_terms,order_by='relevance'):
        """ Performs a search using the metacafe api."""
        try:
            self._logger.info('Hey!listen up!...Attempting to perform a search in MegaVideo')
            self._logger.debug('Search Terms: %s',search_terms)
            srv = MegaVideoService(self._logger)
            query = MegaVideoQuery()
            query.query = search_terms
            query.sort = order_by
            query.max_results=int(self.search_max_results)
            return srv.query(query)
        except:
            self._logger.exception('Dammit!...An error ocurred while searching in MegaVideo...')
            return None
        else:
            self._logger.info('Great!...The MegaVideo search was succesfull...')

class MegaVideoService:
    
    def __init__(self,logger):
        self.service_host='www.megavideo.com'
        self._logger=logger
    
    def get_video_entry(self,video_id):
        url='/?v='+video_id
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        entry=self._get_video_details(content)
        entry.url=self.service_host+url
        self._logger.info('Parsed Megavideo video at url: %s',url)
        return entry
    
    def query(self,megavideo_query):
        #httplib.HTTPConnection.debuglevel = 1
        url=megavideo_query.get_url()
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        results=self._get_megavideo_videos_from_content(content,megavideo_query.max_results)
        
        if len(results)==20 and len(results) < megavideo_query.max_results:
            self._logger.info('20 videos founded at Megavideo, searching for more...')
            megavideo_query.page=megavideo_query.page+1
            megavideo_query.max_results=megavideo_query.max_results-20
            return (results + self.query(megavideo_query))
        else:    
            return results
    
    def _make_http_request(self,url):
        conn = httplib.HTTPConnection(self.service_host)
        custom_headers = {'User-agent': choice(CustomUrlOpener._user_agents)}
        conn.request('GET',url,headers=custom_headers)  
        return conn.getresponse()
    
    
    
    def _get_video_details(self,html_data):
        soup= BeautifulSoup(''.join(html_data),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
        script=soup.find('script',text=re.compile('flashvars'))
        
        title=re.compile('flashvars.title = "(.+?)";').findall(script.string)
        description=re.compile('flashvars.description = "(.+?)";').findall(script.string)
        tags=re.compile('flashvars.tags = "(.+?)";').findall(script.string)
        category=re.compile('flashvars.category = "(.+?)";').findall(script.string)
        
        video=MegaVideoVideo()
        video.title=strip_accents(urllib.unquote(title[0].replace('+', ' ')))
        video.description=strip_accents(urllib.unquote(description[0].replace('+', ' ')))
        video.category=strip_accents(urllib.unquote(category[0].replace('+', ' ')))
        video.tags=strip_accents(urllib.unquote(tags[0].replace('+', ' ')))
        
        return video

    def _get_megavideo_videos_from_content(self,html_data,count):
       links=self._get_video_links(html_data)
       videos=[]
       i=0
       for link in links:
           if i < count:
               i=i+1
               video_id=_get_video_id(link)
               try:
                   videos.append(self.get_video_entry(video_id))
               except:
                   self._logger.exception('An error occurred while parsing video:%s ... Moving on to the next video...',video_id)
                   continue
           else:
               break    
               
       return videos

    def _get_video_links(self,html_data):
        soup = BeautifulSoup(''.join(html_data),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
        link_tds=soup.findAll('td',width='420')
        link_a=[]
        for td in link_tds:
            link_a.append(td.find('a')['href'])
        return link_a
    

        

class MegaVideoVideo:
    
    def __init__(self):
        self.title=None
        self.description=None
        self.tags=None
        self.url=None
        self.category=None
        

class MegaVideoQuery:
    
    def __init__(self):
        self.query=None
        self.max_results=None
        self.sort=None
        self.page=1
    
    def get_url(self):
        
        url='/?c=search&s='+ self.query.replace(' ','+')
        
        if self.sort!=None and self.sort!='relevance':
            url=url+'&sort='+self.sort

        if self.page > 1:
            url=url+'&p='+str(self.page)

        return url

def _get_video_id(url):
    b,s,a=url.partition('=')
    return a