'''
Created on Aug 30, 2009

@author: eorbe
'''

from utils.BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
from downloaders.base_downloader import BaseDownloader,CustomUrlOpener
import urllib
import re

class MegaVideoDownloader(BaseDownloader):
    
    def __init(self,download_record,folder_path,logger,retries=1):
        BaseDownloader.__init__(self, download_record,folder_path,logger,retries)
        

    def start(self):
        """Starts the downloading of the different qualities of a video. Returns a list with the results."""
        url=self.download_record['urls'][0]['url']
        video_id=self._get_video_id(url)
        lq_url=self._get_video_info(video_id)
        
        results=[]
        
        if lq_url!=None:
            self._logger.info('Let''s download the low quality version of the video at url %s...',lq_url)
            self.download(lq_url, self._get_file_name('0'))
        else:
            self.download_ok=False
            self._logger.info('The Megavideo video url couldn''t be found. Maybe Megavideo changed their html. The page url is %s',url)
            self.download_error='The video url couldn''t be found. Maybe MegaVideo changed their html or protocol.'

        results.append(self._build_result_record(lq_url, '0')) 
        
        return results
    
    def _get_video_id(self,url):
        b,s,a=url.partition('=')
        return a
    
    def _get_video_info(self,video_id):
        opener=CustomUrlOpener()
        url='http://www.megavideo.com/xml/videolink.php?v='+video_id
        page=opener.open(url)
        response=page.read()
        page.close()
        
        errort = re.compile(' errortext="(.+?)"').findall(response)
        if len(errort) <= 0:
            s = re.compile(' s="(.+?)"').findall(response)
            k1 = re.compile(' k1="(.+?)"').findall(response)
            k2 = re.compile(' k2="(.+?)"').findall(response)
            un = re.compile(' un="(.+?)"').findall(response)
            video_url='http://www' + s[0] + '.megavideo.com/files/' + self._decrypt(un[0], k1[0], k2[0]) + '/?.flv'
            return video_url
        else:
            self._logger.error('We couldn''t get the megavideo url of this video: %s',video_id)
            return None
    
    def _ajoin(self,arr):
        strtest = ''
        for num in range(len(arr)):
            strtest = strtest + str(arr[num])
        return strtest

    def _asplit(self,mystring):
        arr = []
        for num in range(len(mystring)):
            arr.append(mystring[num])
        return arr
        

    def _decrypt(self,str1, key1, key2):
    
        __reg1 = []
        __reg3 = 0
        while (__reg3 < len(str1)):
            __reg0 = str1[__reg3]
            holder = __reg0
            if (holder == "0"):
                __reg1.append("0000")
            else:
                if (__reg0 == "1"):
                    __reg1.append("0001")
                else:
                    if (__reg0 == "2"): 
                        __reg1.append("0010")
                    else: 
                        if (__reg0 == "3"):
                            __reg1.append("0011")
                        else: 
                            if (__reg0 == "4"):
                                __reg1.append("0100")
                            else: 
                                if (__reg0 == "5"):
                                    __reg1.append("0101")
                                else: 
                                    if (__reg0 == "6"):
                                        __reg1.append("0110")
                                    else: 
                                        if (__reg0 == "7"):
                                            __reg1.append("0111")
                                        else: 
                                            if (__reg0 == "8"):
                                                __reg1.append("1000")
                                            else: 
                                                if (__reg0 == "9"):
                                                    __reg1.append("1001")
                                                else: 
                                                    if (__reg0 == "a"):
                                                        __reg1.append("1010")
                                                    else: 
                                                        if (__reg0 == "b"):
                                                            __reg1.append("1011")
                                                        else: 
                                                            if (__reg0 == "c"):
                                                                __reg1.append("1100")
                                                            else: 
                                                                if (__reg0 == "d"):
                                                                    __reg1.append("1101")
                                                                else: 
                                                                    if (__reg0 == "e"):
                                                                        __reg1.append("1110")
                                                                    else: 
                                                                        if (__reg0 == "f"):
                                                                            __reg1.append("1111")
    
            __reg3 = __reg3 + 1
    
        mtstr = self._ajoin(__reg1)
        __reg1 = self._asplit(mtstr)
        __reg6 = []
        __reg3 = 0
        while (__reg3 < 384):
        
            key1 = (int(key1) * 11 + 77213) % 81371
            key2 = (int(key2) * 17 + 92717) % 192811
            __reg6.append((int(key1) + int(key2)) % 128)
            __reg3 = __reg3 + 1
        
        __reg3 = 256
        while (__reg3 >= 0):
    
            __reg5 = __reg6[__reg3]
            __reg4 = __reg3 % 128
            __reg8 = __reg1[__reg5]
            __reg1[__reg5] = __reg1[__reg4]
            __reg1[__reg4] = __reg8
            __reg3 = __reg3 - 1
        
        __reg3 = 0
        while (__reg3 < 128):
        
            __reg1[__reg3] = int(__reg1[__reg3]) ^ int(__reg6[__reg3 + 256]) & 1
            __reg3 = __reg3 + 1
    
        __reg12 = self._ajoin(__reg1)
        __reg7 = []
        __reg3 = 0
        while (__reg3 < len(__reg12)):
    
            __reg9 = __reg12[__reg3:__reg3 + 4]
            __reg7.append(__reg9)
            __reg3 = __reg3 + 4
            
        
        __reg2 = []
        __reg3 = 0
        while (__reg3 < len(__reg7)):
            __reg0 = __reg7[__reg3]
            holder2 = __reg0
        
            if (holder2 == "0000"):
                __reg2.append("0")
            else: 
                if (__reg0 == "0001"):
                    __reg2.append("1")
                else: 
                    if (__reg0 == "0010"):
                        __reg2.append("2")
                    else: 
                        if (__reg0 == "0011"):
                            __reg2.append("3")
                        else: 
                            if (__reg0 == "0100"):
                                __reg2.append("4")
                            else: 
                                if (__reg0 == "0101"): 
                                    __reg2.append("5")
                                else: 
                                    if (__reg0 == "0110"): 
                                        __reg2.append("6")
                                    else: 
                                        if (__reg0 == "0111"): 
                                            __reg2.append("7")
                                        else: 
                                            if (__reg0 == "1000"): 
                                                __reg2.append("8")
                                            else: 
                                                if (__reg0 == "1001"): 
                                                    __reg2.append("9")
                                                else: 
                                                    if (__reg0 == "1010"): 
                                                        __reg2.append("a")
                                                    else: 
                                                        if (__reg0 == "1011"): 
                                                            __reg2.append("b")
                                                        else: 
                                                            if (__reg0 == "1100"): 
                                                                __reg2.append("c")
                                                            else: 
                                                                if (__reg0 == "1101"): 
                                                                    __reg2.append("d")
                                                                else: 
                                                                    if (__reg0 == "1110"): 
                                                                        __reg2.append("e")
                                                                    else: 
                                                                        if (__reg0 == "1111"): 
                                                                            __reg2.append("f")
                                                                        
            __reg3 = __reg3 + 1
    
        endstr = self._ajoin(__reg2)
        return endstr
    