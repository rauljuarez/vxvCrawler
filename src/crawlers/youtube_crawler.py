'''
Created on Aug 5, 2009

@author: eorbe
'''
import gdata.youtube
import gdata.youtube.service
import logging
from crawlers.base_crawler import BaseCrawler
from utils.funcs import strip_accents
    
class YouTubeCrawler(BaseCrawler):
    
    def __init__(self,logger,search_id,search_max_results):
        BaseCrawler.__init__(self,logger,search_id,int(search_max_results))
        self.start_index=1
        
    def search(self,search_terms,order_by='viewCount'):
        """ Performs a search in YouTube an return a dictionary with the relevant metadata."""
        results=self._do_search(search_terms, order_by)
        if results!=None:
            return self._parse_feed(results)
        else:
            return []
    
    def get_video_metadata(self,vid,url):
        """ Retrieves the metada from a video located at url passed as an argument"""
        try:
            self._logger.info('Ok!...Lets try to retrieve some metada from YouTube')
            id=self._get_video_id(url)
            if id!='':
                srv = gdata.youtube.service.YouTubeService()
                item_meta=self._parse_entry(srv.GetYouTubeVideoEntry(video_id=id))
                #item_meta['search-id']=0
                item_meta['video-id']=vid
                return item_meta
            else:
                self._logger.error('Ouch!...An illegal url was provided. It was impossible to get the video id.')
                return None
        except:
            self._logger.exception('Dammit!...An error ocurred while retrieving metadata from YouTube...')
            return None
        else:
            self._logger.info('Great!...The YouTube search was succesfull...')
          
    def _get_video_id(self,url):
        b,s,a=url.partition('=')
        return a  
    
    def _parse_page(self,page_url):
        return None,None,None;
    
           
    def _parse_feed(self,feed):
        """ Collects the relevant metadata from each entry of the search results feed.""" 
        meta=[]
        for entry in feed.entry:
            item_meta=self._parse_entry(entry)
            #item_meta['search-id']=self.search_id
            item_meta['video-id']='0'
            meta.append(item_meta)
        self._logger.info('%s videos were founded and parsed at YouTube',len(meta))    
        return meta

    def _parse_entry(self,entry):
        """ Collects the relevant metadata from a search result entry."""
        lq_url,hq_url,hd_url=self._parse_page(entry.media.player.url)                   
        item_meta={'title':strip_accents(entry.media.title.text),
                   'description':strip_accents(entry.media.description.text),
                   'category':strip_accents(entry.media.category[0].text),
                   'tags':strip_accents(entry.media.keywords.text),
                   'page_url':entry.media.player.url,
                   'lq_url':lq_url,
                   'hq_url':hq_url,
                   'hd_url':hd_url,
                   'search-id':self.search_id,
                   'source':'1',}
        self._logger.info('Parsed youtube video at url: %s',entry.media.player.url)
        self._logger.debug('Video Metadata: %s',item_meta)
        return item_meta
   
    def _do_search(self,search_terms,order_by='viewCount',max_results=50):
        """ Performs a search using the youtube api."""
        try:
            self._logger.info('Hey!listen up!...Attempting to perform a search in YouTube')
            self._logger.debug('Search Terms: %s',search_terms)
            srv = gdata.youtube.service.YouTubeService()
            query = gdata.youtube.service.YouTubeVideoQuery()
            query.vq = search_terms
            query.orderby = order_by
            query.start_index=self.start_index
            query.max_results=max_results
            
            results=srv.YouTubeQuery(query)
        
            if len(results.entry)==50 and len(results.entry) < self.search_max_results:
                self._logger.info('50 videos founded at YouTube, searching for more...')
                self.start_index=self.start_index+50
                self.search_max_results=self.search_max_results - 50
                new_max_results=50
                if self.search_max_results<50:
                    new_max_results=self.search_max_results
                results.entry=results.entry + self._do_search(search_terms, order_by,new_max_results).entry
                #return (results + self._do_search(search_terms, order_by))
                return results
            else:    
                return results
        
        
        except:
            self._logger.exception('Dammit!...An error ocurred while searching in YouTube...')
            return None
        else:
            self._logger.info('Great!...The YouTube search was succesfull...')
    
                              