'''
Created on Sep 1, 2009

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

class DailyMotionCrawler(BaseCrawler):
    #http://www.dailymotion.com/?c=search&s=
    def __init__(self,logger,search_id,search_max_results):
        BaseCrawler.__init__(self,logger,search_id,search_max_results)
    
    def search(self,search_terms,order_by='relevance'):
        """ Performs a search in DailyMotion an return a dictionary with the relevant metadata."""
        results=self._do_search(search_terms, order_by)
        if results!=None:
            return self._parse_feed(results)
        else:
            return []
    
    def get_video_metadata(self,vid,url):
        """ Retrieves the metada from a video located at url passed as an argument"""
        try:
            self._logger.info('Ok!...Lets try to retrieve some metada from DailyMotion')
            id=_get_video_id(url)
            if id!='':
                srv = DailyMotionService(self._logger)
                item_meta=self._parse_entry(srv.get_video_entry(id))
                item_meta['video-id']=str(vid)
                return item_meta
            else:
                self._logger.error('Ouch!...An illegal url was provided. It was impossible to get the video id.')
                return None
        except:
            self._logger.exception('Dammit!...An error ocurred while retrieving metadata from DailyMotion...')
            return None
        else:
            self._logger.info('Great!...The DailyMotion search was succesfull...')
            
    def _parse_feed(self,feed):
        """ Collects the relevant metadata from each entry of the search results feed.""" 
        meta=[]
        for entry in feed:
            item_meta=self._parse_entry(entry)
            item_meta['video-id']='0'
            meta.append(item_meta)
        self._logger.info('%s videos were founded and parsed at Dailymotion',len(meta))    
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
                   'source':'5',}
        self._logger.debug('Video Metadata: %s',item_meta)
        return item_meta
   
    def _do_search(self,search_terms,order_by='relevance'):
        """ Performs a search using the metacafe api."""
        try:
            self._logger.info('Hey!listen up!...Attempting to perform a search in DailyMotion')
            self._logger.debug('Search Terms: %s',search_terms)
            srv = DailyMotionService(self._logger)
            query = DailyMotionQuery()
            query.query = search_terms
            query.sort = order_by
            query.max_results=int(self.search_max_results)
            return srv.query(query)
        except:
            self._logger.exception('Dammit!...An error ocurred while searching in DailyMotion...')
            return None
        else:
            self._logger.info('Great!...The DailyMotion search was succesfull...')

class DailyMotionService:
    
    def __init__(self,logger):
        self.service_host='www.dailymotion.com'
        self._logger=logger
    
    def get_video_entry(self,video_id):
        url='/video/'+video_id
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        entry=self._get_video_details(content)
        entry.url='http://'+self.service_host+url
        self._logger.info('Parsed dailymotion video at url: %s',url)
        return entry
    
    def query(self,dailymotion_query):
        #httplib.HTTPConnection.debuglevel = 1
        url=dailymotion_query.get_url()
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        results=self._get_dailymotion_videos_from_content(content,dailymotion_query.max_results)
        
        if len(results)==16 and len(results) < dailymotion_query.max_results:
            self._logger.info('16 videos founded at Dailymotion, searching for more...')
            dailymotion_query.page=dailymotion_query.page+1
            dailymotion_query.max_results=dailymotion_query.max_results-16
            return (results + self.query(dailymotion_query))
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
        
        t=soup.find('h1',{'class':'dmco_title'})
        title=t.string if t != None else ''
        
        d=soup.find('div',id='video_description')
        description=d.string if d!=None else None
        
        c=soup.find('a',{'class':re.compile('fromchannel_link')})
        category=c.string if c!=None else None
        
        tags_el=soup.find('div',{'class':re.compile('tags_cont')}).findAll('a')
        tags_list=[]
        for a in tags_el:
            tags_list.append(a.string)
        tags=','.join(tags_list)    
            
        
        video=DailyMotionVideo()
        video.title=strip_accents(title)
        video.description=strip_accents(description) if description!=None else None
        video.category=strip_accents(category)
        video.tags=strip_accents(tags)
        
        return video

    def _get_dailymotion_videos_from_content(self,html_data,count):
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
        div=soup.find('div',id='dual_list')
        link_divs=div.findAll('div',{'class':re.compile('^dmpi_video_item')})
        link_a=[]
        for div in link_divs:
            link_a.append(div.find('a',{'class':re.compile('^dmco_simplelink video_title')})['href'])
        return link_a

class DailyMotionVideo:
    
    def __init__(self):
        self.title=None
        self.description=None
        self.tags=None
        self.url=None
        self.category=None

class DailyMotionQuery:
    
    def __init__(self):
        self.query=None
        self.max_results=None
        self.sort='relevance'
        self.page=1
    
    def get_url(self):
        
        url='/search/'+ self.query.replace(' ','+')+'/'
        
        if self.sort!=None:
            url='/'+self.sort+url

        if self.page > 1:
            url=url+str(self.page)

        return url

def _get_video_id(url):
    b,s,a=url.rpartition('/')
    return a