#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import hashlib
import urllib
import httplib
from random import choice
from xml.etree import cElementTree as ElementTree
from crawlers.base_crawler import BaseCrawler,CustomUrlOpener
from utils.funcs import strip_accents

class MetacafeCrawler(BaseCrawler):
    
    def __init__(self,logger,search_id,search_max_results):
        BaseCrawler.__init__(self,logger,search_id,search_max_results)
        
    def search(self,search_terms,order_by='rating'):
        """ Performs a search in Metacafe an return a dictionary with the relevant metadata."""
        results=self._do_search(search_terms, order_by)
        if results!=None:
            return self._parse_feed(results)
        else:
            return []
    
    def get_video_metadata(self,vid,url):
        """ Retrieves the metada from a video located at url passed as an argument"""
        try:
            self._logger.info('Ok!...Lets try to retrieve some metada from Metacafe')
            id=self._get_video_id(url)
            if id!='':
                srv = MetacafeService(self._logger)
                item_meta=self._parse_entry(srv.get_video_entry(id))
                item_meta['video-id']=str(vid)
                return item_meta
            else:
                self._logger.error('Ouch!...An illegal url was provided. It was impossible to get the video id.')
                return None
        except:
            self._logger.exception('Dammit!...An error ocurred while retrieving metadata from Metacafe...')
            return None
        else:
            self._logger.info('Great!...The Metacafe search was succesfull...')
          
    def _get_video_id(self,url):
        a=url.split('/')
        if a!=None:
            return a[-3]
        else:
            return None
    
  
           
    def _parse_feed(self,feed):
        """ Collects the relevant metadata from each entry of the search results feed.""" 
        meta=[]
        for entry in feed:
            item_meta=self._parse_entry(entry)
            item_meta['video-id']='0'
            meta.append(item_meta)
        self._logger.info('%s videos were founded and parsed at Metacafe',len(meta))    
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
                   'source':entry.source,}
        self._logger.debug('Video Metadata: %s',item_meta)
        return item_meta
   
    def _do_search(self,search_terms,order_by='rating'):
        """ Performs a search using the metacafe api."""
        try:
            self._logger.info('Hey!listen up!...Attempting to perform a search in Metacafe')
            self._logger.debug('Search Terms: %s',search_terms)
            srv = MetacafeService(self._logger)
            query = MetacafeQuery()
            query.query = search_terms
            query.sort = order_by
            query.max_results=int(self.search_max_results)
            return srv.query(query)
        except:
            self._logger.exception('Dammit!...An error ocurred while searching in Metacafe...')
            return None
        else:
            self._logger.info('Great!...The Metacafe search was succesfull...')


class MetacafeService:
    
    def __init__(self,logger):
        self.service_host='www.metacafe.com'
        self.service_base_url='/tags/'
        self._logger=logger=logger
    
    def get_video_entry(self,vid):
        url='/api/item/%s/' % vid
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        entry=self._get_metacafe_videos_from_content(content,1)
        if entry!=None and len(entry)>0:
            self._logger.info('Parsed metacafe video at url: %s',entry[0].url)
            return entry[0]
        else:
            return None
    
    def query(self,metacafe_query):
        #httplib.HTTPConnection.debuglevel = 1
        url=self.service_base_url+metacafe_query.get_url()+'rss.xml'
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        results=self._get_metacafe_videos_from_content(content,metacafe_query.max_results)
        
        if len(results)==20 and len(results) < metacafe_query.max_results:
            self._logger.info('20 videos founded at Metacafe, searching for more...')
            metacafe_query.page=metacafe_query.page+1
            metacafe_query.max_results=metacafe_query.max_results-20
            return (results + self.query(metacafe_query))
        else:    
            return results
    
    def _make_http_request(self,url):
        conn = httplib.HTTPConnection(self.service_host)
        custom_headers = {'User-agent': choice(CustomUrlOpener._user_agents)}
        conn.request('GET',url,headers=custom_headers)  
        return conn.getresponse()

    def _get_metacafe_videos_from_content(self,xml_data,count):
        #load the xml in memory
        sanitized_xml_data=''.join([c for c in xml_data if ord(c)<128])
        
        tree=ElementTree.fromstring(sanitized_xml_data)
        videos = []
        n=0
        for i, elem in enumerate(tree.getiterator('item')):
            if n < count:
                n=n+1
                try:
                    video=MetacafeVideo()
                    video.title=strip_accents(elem.find('title').text)
                    video.description=strip_accents(elem.find('description').text)
                    video.url=strip_accents(elem.find('link').text)
                    other_source,source_id,new_url=self._verify_source_of_video(video.url)
                    if other_source:
                        video.url=new_url
                        video.source=source_id
                        
                    video.category=strip_accents(elem.find('category').text)
                    video.tags=strip_accents(elem.find('{http://search.yahoo.com/mrss/}keywords').text)
                    videos.append(video)
                    self._logger.info('Parsed metacafe video at url: %s',video.url)
                except:
                    self._logger.exception('An error occurred while parsing a video ... Moving on to the next video...')
                    continue
            else:
                break 
        return videos
    
    def _verify_source_of_video(self,url):
        url=url.rstrip('/')
        a,b,vid=url.rpartition('/')
        a,b,vid=a.rpartition('/')
        if vid.startswith('yt'):
            c,d,youtube_id=vid.partition('-')
            return True,'1','http://www.youtube.com/watch?v='+youtube_id
        else:
            return False,None,None    


class MetacafeVideo:
    
    def __init__(self):
        self.title=None
        self.description=None
        self.tags=None
        self.url=None
        self.category=None
        self.source='3'
        

class MetacafeQuery:
    
    def __init__(self):
        self.query=None
        self.max_results=None
        self.sort=None
        self.page=1
    
    def get_url(self):
        
        url=self.query.replace(' ','_')+'/'
        
        if self.sort!=None and self.sort!='rating':
            url=url+self.sort+'/'

        if self.page > 1:
            url=url+'page-'+str(self.page)+'/'

        return url
    
            
        
       
