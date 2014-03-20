import httplib
import urllib
from xml.etree import cElementTree as ElementTree

class VXVService:
    
    def __init__(self,service_host,client_id,client_type):
        self.service_host=service_host
        self.service_base_url='/api/'
        self._client_id=client_id
        self._client_type=client_type
        self.service_available=True
    
    def get_configuration(self):
        url=('/api/config/%s/%s/' % (self._client_type,self._client_id))
        f=self._make_http_request('GET',urllib.quote(url,'/=?&'))
        content=f.read()
        child_count,parsed_content=self._parse_configuration(content)
        self.service_available=(child_count>0)
        return self.service_available,self._client_id,parsed_content
        
    def get_search_batch(self):
        url=('/api/search/batch/%s/%s/' % (self._client_type,self._client_id))
        f=self._make_http_request('GET',urllib.quote(url,'/=?&'))
        content=f.read()
        return self._parse_search_batch(content)
    
    def get_download_batch(self):
        url=('/api/download/batch/%s/%s/' % (self._client_type,self._client_id))
        f=self._make_http_request('GET',urllib.quote(url,'/=?&'))
        content=f.read()
        return self._parse_download_batch(content)
    
    def send_search_results(self,content):
        url=('/api/search/results/%s/%s/' % (self._client_type,self._client_id))
        f=self._make_http_request('POST',urllib.quote(url,'/=?&'),content)
        content=f.read()
        return self._parse_response(content)
    
    def send_download_results(self,content):
        url=('/api/download/results/%s/%s/' % (self._client_type,self._client_id))
        f=self._make_http_request('POST',urllib.quote(url,'/=?&'),content)
        content=f.read()
        return self._parse_response(content)
    
    def _set_client_id(self,id):
        self._client_id=id
    
    def _parse_response(self,xml_data):
        sanitized_xml_data=''.join([c for c in xml_data if ord(c)<128])
        tree=ElementTree.fromstring(sanitized_xml_data)
        if tree.get('succeed')=='True':
            return True
        else:
            return False
    
    def _parse_search_batch(self,xml_data):
        sanitized_xml_data=''.join([c for c in xml_data if ord(c)<128])
        tree=ElementTree.fromstring(sanitized_xml_data)
        child_count=tree.getchildren()
        
        return len(child_count),sanitized_xml_data
    
    def _parse_download_batch(self,xml_data):
        sanitized_xml_data=''.join([c for c in xml_data if ord(c)<128])
        tree=ElementTree.fromstring(sanitized_xml_data)
        child_count=tree.getchildren()
        
        return len(child_count),sanitized_xml_data
        
        
    def _parse_configuration(self,xml_data):
        
        sanitized_xml_data=''.join([c for c in xml_data if ord(c)<128])
       
        tree=ElementTree.fromstring(sanitized_xml_data)
        child_count=len(tree.getchildren())
        
        config= {}
        if child_count>0:
            if self._client_id<=0:
                self._set_client_id(tree.get('guid'))
                
            for i, elem in enumerate(tree.getiterator('confvar')):
                key=elem.get('name')
                value=elem.get('value')
                type=elem.get('type')
                if type=='int':
                    config[key]=int(value)
                elif type=='double':
                    config[key]=float(value)
                else: 
                    config[key]=value        
        return child_count,config 
    
    def _make_http_request(self,method,url,body=None):
        #httplib.HTTPConnection.debuglevel = 1
        conn = httplib.HTTPConnection(self.service_host)
        conn.request(method,url,body)
        return conn.getresponse()

if __name__ == '__main__':
    srv=VXVService('localhost:8000',0,1)
    print srv.get_configuration()
    print srv.get_download_batch()
    print srv.get_search_batch()
    