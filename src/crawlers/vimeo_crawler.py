#!/usr/bin/python

import logging
import hashlib
import urllib
import httplib

from random import choice
from xml.etree import cElementTree as ElementTree
from crawlers.base_crawler import BaseCrawler,CustomUrlOpener
from utils.funcs import strip_accents

class VimeoCrawler(BaseCrawler):
    
    def __init__(self,logger,search_id,search_max_results,api_key,api_secret):
        BaseCrawler.__init__(self,logger,search_id,search_max_results)
        self.api_key=api_key
        self.api_secret=api_secret
        
    def search(self,search_terms,order_by='relevant'):
        """ Performs a search in Vimeo an return a dictionary with the relevant metadata."""
        results=self._do_search(search_terms, order_by)
        if results!=None:
            return self._parse_feed(results)
        else:
            return []
    
    def get_video_metadata(self,vid,url):
        """ Retrieves the metada from a video located at url passed as an argument"""
        try:
            self._logger.info('Ok!...Lets try to retrieve some metada from Vimeo')
            id=self._get_video_id(url)
            if id!='':
                srv = VimeoService(self.api_key,self.api_secret,self._logger)
                item_meta=self._parse_entry(srv.get_video_entry(id))
                item_meta['video-id']=str(vid)
                return item_meta
            else:
                self._logger.error('Ouch!...An illegal url was provided. It was impossible to get the video id.')
                return None
        except:
            self._logger.exception('Dammit!...An error ocurred while retrieving metadata from Vimeo...')
            return None
        else:
            self._logger.info('Great!...The Vimeo search was succesfull...')
          
    def _get_video_id(self,url):
        b,s,a=url.rpartition('/')
        if b!=None:
            return a
        else:
            return None
    
    def _parse_page(self,page_url):
        return None,None,None;
    
           
    def _parse_feed(self,feed):
        """ Collects the relevant metadata from each entry of the search results feed.""" 
        meta=[]
        for entry in feed:
            item_meta=self._parse_entry(entry)
            item_meta['video-id']='0'
            meta.append(item_meta)
        self._logger.info('%s videos were founded and parsed at Vimeo',len(meta))     
        return meta

    def _parse_entry(self,entry):
        """ Collects the relevant metadata from a search result entry."""
        item_meta={'title':entry.title,
                   'description':entry.description,
                   'category':'',
                   'tags':','.join(entry.tags),
                   'page_url':entry.urls[0],
                   'lq_url':None,
                   'hq_url':None,
                   'hd_url':None,
                   'search-id':self.search_id,
                   'source':'2',}
        self._logger.debug('Video Metadata: %s',item_meta)
        return item_meta
   
    def _do_search(self,search_terms,order_by='relevant'):
        """ Performs a search using the vimeo api."""
        try:
            self._logger.info('Hey!listen up!...Attempting to perform a search in Vimeo')
            self._logger.debug('Search Terms: %s',search_terms)
            srv = VimeoService(self.api_key,self.api_secret,self._logger)
            query = VimeoQuery()
            query.query = search_terms
            query.sort = order_by
            query.full_response=True
            query.max_results=int(self.search_max_results)
            return srv.query(query)
        except:
            self._logger.exception('Dammit!...An error ocurred while searching in Vimeo...')
            return None
        else:
            self._logger.info('Great!...The Vimeo search was succesfull...')


class VimeoService:
    
    def __init__(self,api_key,api_secret,logger):
        self.api_key=api_key
        self.api_secret=api_secret
        self.service_host='www.vimeo.com'
        self.service_base_url='/api/rest/v2/'
        self._logger=logger
        #self._urlopener=CustomUrlOpener()
    
    def get_video_entry(self,vid):
        
        signature=self._make_signature('vimeo.videos.getInfo', self._get_query_strings(for_signature=True,video_id=vid))
        url=self._build_url(self._get_query_strings(method='vimeo.videos.getInfo',video_id=vid), signature)
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        entry=self._get_vimeo_videos_from_content(content,1)
        if entry!=None and len(entry)>0:
            self._logger.info('Parsed vimeo video at url: %s',entry[0].url)
            return entry[0]
        else:
            return None
    
    def query(self,vimeo_query):
        signature=self._make_signature(vimeo_query.method, vimeo_query.get_query_params(True))
        url=self._build_url(vimeo_query.get_query_params(), signature)
        f=self._make_http_request(urllib.quote(url,'/=?&'))
        content=f.read()
        results=self._get_vimeo_videos_from_content(content,vimeo_query.max_results)
        
        if len(results)==50 and len(results) < vimeo_query.max_results:
            self._logger.info('50 videos founded at Vimeo, searching for more...')
            vimeo_query.page=vimeo_query.page+1
            vimeo_query.max_results=vimeo_query.max_results-50
            return (results + self.query(vimeo_query))
        else:    
            return results
        
        
    
    def _make_http_request(self,url):
        conn = httplib.HTTPConnection(self.service_host)
        custom_headers = {'User-agent': choice(CustomUrlOpener._user_agents)}
        conn.request('GET',url,headers=custom_headers)  
        return conn.getresponse()
        
        
    def _build_url(self,query_string,signature):
        return '%s?api_key=%s&%s&api_sig=%s' % (self.service_base_url,self.api_key,query_string,signature)
    
    def _get_query_strings(self,for_signature=False,**kwargs):
        query_string=[]
        for param,val in kwargs.items():
            query_string.append(('%s=%s' % (param,val)))
        
            
        if for_signature:
            return ''.join(query_string).replace('=','')
        else:
            return '&'.join(query_string)
                
        
    def _make_signature(self,method,params,**kwargs):
        base_sig=('%sapi_key%smethod%s' % (self.api_secret,self.api_key,method))
        for param,val in kwargs.items():
            params=params+param+val
        signature=hashlib.md5(base_sig+params).hexdigest()
        
        return signature

    def _get_vimeo_videos_from_content(self,xml_data,count):
        #load the xml in memory
        sanitized_xml_data=''.join([c for c in xml_data if ord(c)<128])
       
        tree=ElementTree.fromstring(sanitized_xml_data)
        videos = []
        n=0
        for i, elem in enumerate(tree.getiterator('video')):
            if n < count:
                n=n+1
                try:
                    video=VimeoVideo()
                    video.title=strip_accents(elem.find('title').text)
                    video.description=strip_accents(elem.find('caption').text)
                    
                    urls=elem.find('urls')
                    if urls!=None:
                        for url in urls.findall('url'):
                            video.urls.append(strip_accents(url.text))
                    
                    tags=elem.find('tags')
                    if tags!=None:
                        for tag in tags.findall('tag'):
                            video.tags.append(strip_accents(tag.text))
                    
                    videos.append(video)
                    self._logger.info('Parsed vimeo video at url: %s',video.urls)
                except:
                        self._logger.exception('An error occurred while parsing a video ... Moving on to the next video...')
                        continue 
            else:
                break
        return videos

class VimeoVideo:
    
    def __init__(self):
        self.title=None
        self.description=None
        self.tags=[]
        self.urls=[]

class VimeoQuery:
    
    def __init__(self):
        self.query=None
        self.full_response=False
        self.page=1
        self.per_page=50
        self.sort=None
        self.user_id=None
        self.method='vimeo.videos.search'
        self.max_results=50
    
    def get_query_params(self,for_signature=False):
        params='method=%s&query=%s'% (self.method,self.query.replace(' ','+'))
        
        if self.full_response:
            params=params+'&full_response=true'
            
        if self.page!=None:
            params=params+('&page=%s' % self.page)

        if self.per_page!=None:
            params=params+('&per_page=%s' % self.per_page)
            
        if self.sort!=None:
            params=params+'&sort='+self.sort

        if self.user_id!=None:
            params=params+('&user_id=%s' % self.user_id)
        
        if for_signature:
            return params.replace('=','')
        else:
            return params
    
            
        
       