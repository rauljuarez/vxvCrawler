'''
Created on Aug 5, 2009

@author: eorbe
'''
import sys
import os.path
import logging
import utils.distributed_logging
import cPickle
import zipfile
import traceback
import time
from xml.dom.minidom import Document
from crawlers.youtube_crawler import YouTubeCrawler
from crawlers.vimeo_crawler import VimeoCrawler
from crawlers.metacafe_crawler import MetacafeCrawler
from crawlers.megavideo_crawler import MegaVideoCrawler
from crawlers.dailymotion_crawler import DailyMotionCrawler
from utils.vxv_service import VXVService
try:
    from xml.etree import cElementTree as ElementTree
except ImportError, e:
    from xml.etree import ElementTree





logger=None

def _config_logging(queue=None):
    if queue!=None:
        return utils.distributed_logging.QueueLogger(queue,'crawlers.base')
    else:
        return logging.getLogger('crawlers.base')

def batch_poller(service_host, client_id,poll_time,queue_folder):
    _logger=logging.getLogger('batch_poller')
    _logger.info('Polling the service for a search batch...')
    srv=VXVService(service_host, client_id, 0)
    count,xml_data=srv.get_search_batch()
    if count>0:
        try:
            time.sleep(5)
            _logger.info('There is a search batch available, saving it to the disk...')
            file_name=os.path.join(queue_folder, ('search-batch-%s.xml' % int(time.time())))
            f=open(file_name,'w')
            f.write(xml_data)
            f.close()
        except:
            _logger.exception('An error occurred while saving the received search batch...Exiting...')
            sys.exit(1)
    else:
        _logger.info('There is no search batch available. Polling again in %s seconds..',poll_time)
    time.sleep(poll_time)
    batch_poller(service_host, client_id,poll_time,queue_folder)
    
def batch_processor(config,task_queue,logging_queue=None):    
    global logger
    
    logger=_config_logging(logging_queue)
    
    if task_queue!=None:
        while True:
            #get the file from the pool
            batch_file_name=task_queue.get()
            if batch_file_name != None:
                try:
                    base_file_name=os.path.splitext(batch_file_name)[0]

                    logger.info('Starting the processing of batch file %s ...',batch_file_name)
                    sbatch=parse_xml_search_batch(os.path.join(config['queuePath'],batch_file_name))
                    
                    result_entries=[]
                    logger.debug('Search Batch: %s', sbatch)
                    for search in sbatch['searches']:
                        crawler=get_crawler(search['source'],search['search-id'], logger, search['max-results'],config)
                        result_entries.extend(crawler.search(search['terms']))
                    
                    for parse in sbatch['parses']:
                        crawler=get_crawler(parse['source'],0,logger,0,config)
                        result_entries.append(crawler.get_video_metadata(parse['video-id'], parse['url']))   
                    
                    logger.debug('Search Results:%s',result_entries)
                    
                    results=[r for r in result_entries if r!=None]
                    
                    results_xml=build_xml_results_batch(results, config)
                    
                    send_ok=send_results_to_service(results_xml,base_file_name,config)
                    #send_ok=True
                    if send_ok:
                        clean_up(base_file_name,config)
                    
                except:
                    logger.exception('An error occurred while processing batch file %s ....',batch_file_name)
            else:
                time.sleep(10)    
    else:
        logger.error('You must provide a task queue if you intend to run the processor in a multithreading fashion.')
        sys.exit(1)
        

def clean_up(batch_file_name,config):
    
    logger.info('Uuhgggg!, this is a messs...let''s make some cleanup...')

    tarf=zipfile.ZipFile(os.path.join(config['processedPath'],(batch_file_name+".zip")), "w")
    fname1=os.path.join(config['queuePath'],(batch_file_name+'.xml'))
    fname2=os.path.join(config['queuePath'],(batch_file_name+'-results.rxml'))
    try:
        logger.info('Packing the generated files in the queue folder...')
        tarf.write(fname1)
        tarf.write(fname2)
        logger.info('FYI...You have all the files packed in the ''processed folder'' under the name of %s...',(batch_file_name+".zip"))    
    except:
        logger.exception('WTF!!!...An error occurred while packing the batch files...')
    else:
        try:    
            logger.info('Ok, the time of doom has arrived...let''s delete the unnecesary files...')
            os.remove(fname1)
            os.remove(fname2)
        except:
            logger.exception('Dammit!!!...An error occurred while deleting the unnecesary files...')        
    finally:
        tarf.close()
        
    logger.info('The cleanup is done...')   
    
def send_results_to_service(results,file_name,config):
    try:
        xml_file_name=os.path.join(config['queuePath'],file_name+'-results.rxml')
        max_send_retries=config['max_send_results_retries']

        logger.info('Saving xml results file before sending it to the server. File:%s ...',xml_file_name)
        f=open(xml_file_name,'w')
        f.write(results)
        f.close()
        
        retries=1
        srv=VXVService(config['serviceHost'], config['client_id'],config['client_type'])

        while retries <= max_send_retries:
            logger.info('Sending results to the server. Attempt # %s..',retries)
            if srv.send_search_results(results):
                logger.info('The server received the results correctly...')
                return True
            logger.info('Something happened!!...The server didn''t get the results...trying again...')
            retries=retries+1
        return False
    except:
        logger.exception('Ouch!..An exception occurred while sending the results back to the server...')
    
    
    
def get_crawler(source,search_id,logger,search_max_results,config):
    if source=="1":
        return YouTubeCrawler(logger,search_id, search_max_results)
    elif source=="2":
        return VimeoCrawler(logger, search_id, search_max_results,config['vimeo_api_key'],config['vimeo_api_secret'])
    elif source=="3":
        return MetacafeCrawler(logger, search_id, search_max_results)
    elif source=="4":
        return MegaVideoCrawler(logger, search_id, search_max_results)
    elif source=="5":
        return DailyMotionCrawler(logger, search_id, search_max_results)

    else:
        return None
    
def parse_xml_search_batch(xml_file):
    """ Parses an xml file containing the search batch an return a data structure for internal use."""
    #load the xml in memory
    tree=ElementTree.parse(xml_file)
    results = {}
    for i, elem in enumerate(tree.getiterator('searchs')):
        searchs=[]
        for search in elem.findall('search'):
            searchs.append({'terms':search.text.strip('\n\t'),
                            'search-id':search.get('search-id'),
                            'source':search.get('source'),
                            'max-results':search.get('max-results'),})
        results.update({'searches': searchs,})
        
        parses=[]
        for parse in elem.findall('parse'):
            parses.append({'url':parse.text.strip('\n\t'),
                            'video-id':parse.get('video-id'),
                            'source':parse.get('source'),})
        results.update({'parses': parses,})
    return results

def build_xml_results_batch(results_batch,config):
    """ Takes a data structure containing the results of the searchs and builds an xml representation."""
    doc=Document()
    root=doc.createElement('search_results')
    root.setAttribute('guid',config.get('clientid','0000'))
    doc.appendChild(root)
    
    for result in results_batch:
        video=doc.createElement('video')
        video.setAttribute('search-id',str(result['search-id']))
        video.setAttribute('video-id',str(result['video-id']))
        video.setAttribute('source',result['source'])
        
        title=doc.createElement('title')
        title.appendChild(doc.createTextNode(result['title']))
        video.appendChild(title)
        
        desc=doc.createElement('description')
        if result['description']!=None:
            desc.appendChild(doc.createTextNode(result['description']))
        else:
            desc.appendChild(doc.createTextNode(''))
        video.appendChild(desc)

        cat=doc.createElement('category')
        if result['category']!=None:
            cat.appendChild(doc.createTextNode(result['category']))
        video.appendChild(cat)

        tags=doc.createElement('tags')
        if result['tags']!=None:
            tags.appendChild(doc.createTextNode(result['tags']))
        video.appendChild(tags)

        purl=doc.createElement('page_url')
        purl.appendChild(doc.createTextNode(result['page_url']))
        video.appendChild(purl)
        
        if result['lq_url']!=None:
            lq_url=doc.createElement('lq_url')
            lq_url.appendChild(doc.createTextNode(result['lq_url']))
            video.appendChild(lq_url)

        if result['hq_url']!=None:
            hq_url=doc.createElement('hq_url')
            hq_url.appendChild(doc.createTextNode(result['hq_url']))
            video.appendChild(hq_url)
        
        if result['hd_url']!=None:
            hd_url=doc.createElement('hd_url')
            hd_url.appendChild(doc.createTextNode(result['hd_url']))
            video.appendChild(hd_url)

        root.appendChild(video)
        
    xml_str=doc.toxml()
    doc.unlink()
    sanitized_xml_data=''.join([c for c in xml_str if ord(c)<128])
    return sanitized_xml_data 

