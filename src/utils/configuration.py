'''
Created on Jul 16, 2009

@author: eorbe
'''
import logging
from xml.sax import make_parser
from xml.sax.handler import ContentHandler
from utils.vxv_service import VXVService
import bz2

def LoadXmlConfigurationFromFile(xmlfile):
    p=make_parser()
    h = XmlConfigurationParser()
    p.setContentHandler(h)
    p.parse(xmlfile)
    return h.dict

def LoadAndMergeRemoteConfiguration(config,serviceUrl,client_id,client_type):
    srv_av,new_id,r_conf=LoadXmlConfigurationFromService(serviceUrl,client_id,client_type)
    #merge the dictionaries
    config.update(r_conf)
    return srv_av,new_id,config

def LoadXmlConfigurationFromService(service_host,client_id,client_type):
    srv=VXVService(service_host, client_id, client_type)
    return srv.get_configuration()
    
    #c=dict(download_service_polling=30, ftpServer='digitaltrends.com.ar',ftpPort=21,ftpUser='w1080855',ftpPass='yafueloko123',ftpFolder='public_html/test',max_download_retries=3,max_upload_retries=2,vimeo_api_key='eb1d9fcc529d28571d13bf268434fa30',vimeo_api_secret='a187f070')
    #return c

class XmlConfigurationParser(ContentHandler):
    
    def __init__(self):
        self.root='configuration'
        self.dict={}
        
    def startElement(self, name, attrs):
        """ Start element handler """
        if name!=self.root:
            self._BuildEntry( attrs['name'], attrs['value'], attrs['type'])
    
    def _BuildEntry(self,name,value,type):
        
        if type=='int':
            self.dict[name]=int(value)
        elif type=='double':
            self.dict[name]=float(value)
        else: 
            self.dict[name]=value

def save_pass(file,password):
    try:
        f=open(file, 'w')
        f.write(bz2.compress(password))
        f.close()
    except:
        return False
    else:
        return True
        

def load_pass(file):
    
    try:
        w = open(file, 'r')
        password = bz2.decompress(w.read())
        w.close()
    except:
        return ''
    else:
        return password
    
            

        