#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import sys
import os
import re
from bs4 import BeautifulSoup
import string
import time

#设置请求头
request_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5",
    "Cache-Control": "max-age=0",
    "Connection": "close",
    "Cookie": "_trs_uv=kfw9b79k_6_b2tv; SF_cookie_1=37059734",
    "Host": "www.stats.gov.cn",
    "DNT": "1",
    "If-Modified-Since": "Tue, 04 Jan 2022 07:43:24 GMT",
    "If-None-Match": "1736-5d4bccabedf00-gzip",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0"
}
proxies = {"https": "60.170.111.51:3888", "http": "61.155.4.135:3128","http":"60.207.194.118:80","https":"60.191.11.241:3128","http":"121.232.148.189:9000","http":"218.59.193.14:3864","http":"","http":"175.42.123.185:9999","https":"112.91.75.44:9999","http":"101.132.111.208:8082","http":"223.241.79.174:8118","https":"139.196.152.221:8080","https":"222.129.37.3:5711"}

#数据年份
dataYear=2021
#是否值爬取省、市/州、区/县三个级别数据
simpleData = False

#insert语句的索引，当达到指定值后重新生成insert
sqlSaveIndex = 1
#当一条insert 的 values达到该值后重新生成新的一条insert
sqlSaveIndexEnd = 10000
#保存的文件名
if simpleData:
    saveFileName = "data/areacode%s-simple.sql" % dataYear
else:
    saveFileName = "data/areacode%s-all.sql" % dataYear

provinceReg = ''

#正则匹配meta的charset
patternMetaCharset = re.compile(r'<meta.+?charset=[\'|"]?([^\'"]+)[\'|"]?[?:\s+|>|\/>]', re.IGNORECASE)
allCharset=['utf-8','gbk','gb2312','iso-8859-1','gb18030','utf-16','utf-32']
####function echo() start######
def echo( param,*args ):
    if len(args)==0:
        print(param)
    else:
        for var in args:
            if var=='':
                print(param,end='')
            else:
                print(param)
####function echo() end#######

#省份代码缓存
provinceCodeCache=[]
#####读取省份代码缓存#############
def getProvinceCodeCache():
    global provinceCodeCache
    provinceCodeCache.clear()
    try:
        fpc=open("cache.data","a+",encoding="utf-8")
        cacheResult = fpc.readlines()
        if len(cacheResult)>0:
            for cacheItem in cacheResult:
                cacheItem = cacheItem.strip()
                provinceCodeCache.append(cacheItem)
    except:
        echo("没有缓存...")
    finally:
        fpc.close()
###########写入省份代码缓存#########
def writeProvinceCodeCache(code):
    try:
        fpc=open("cache.data","a+",encoding="utf-8")
        fpc.write("%d\n" % code)#\n用来换行
    except:
        echo("写入缓存出错...")
    finally:
        fpc.close()
###########缓存操作结束############

def writeSql(sql):
    try:
        fp=open(saveFileName,"a+",encoding="utf-8")
        fp.write(sql)#\n用来换行
    finally:
        fp.close()

############function: replaceLastChar 替换末尾的,为;#######################
def replaceLastChar():
    #sed -i 's/,\(\w*$\)/;\1/g' data/areacode.sql
    with open(saveFileName, 'r+') as fo:
        filedata = fo.read(-1)
    if filedata.strip() == '':
        echo("error: content is null")
        sys.exit(0)
    if filedata.strip().endswith(',') == True:
        filedata = filedata.strip().rstrip(',')
        filedata = filedata + ';'
    try:
        fp=open(saveFileName,"w+",encoding="utf-8")
        fp.write(filedata)
    finally:
        fp.close()

def echoinfo(name,code):
    print("areaName: %s，areaCode: %s" % (name,code))

def createTableMySQL():
    create_tb_cmd = '''
    CREATE TABLE IF NOT EXISTS areacode{0} (
        code  varchar(20) PRIMARY KEY NOT NULL COMMENT '地址code',
        area_name  varchar(255) DEFAULT '' COMMENT '名字',
        type  int COMMENT '级别,1:省,2:市/州，3区县，4乡镇，5村',
        parent_code varchar(20) COMMENT '父级code ',
        KEY `areacode_index` (`parent_code`)
    ) DEFAULT CHARSET=utf8 COMMENT='地址表{0}';\n
    '''.format(dataYear)
    return create_tb_cmd

def createTablePgSQL():
    sql = '''
    CREATE TABLE if not exists public.areacode{0} (
        code varchar(20) NULL,
        area_name text NULL,
        "type" integer NULL,
        parent_code varchar(20) NULL,
        CONSTRAINT areacode{0}_pk PRIMARY KEY (code)
    );
    CREATE INDEX areacode{0}_parent_code_idx ON public.areacode{0} (parent_code);
    CREATE INDEX areacode{0}_type_idx ON public.areacode{0} ("type");
    COMMENT ON TABLE public.areacode{0} IS '地址表{0}';
    COMMENT ON COLUMN public.areacode{0}.code IS '地址code';
    COMMENT ON COLUMN public.areacode{0}.area_name IS '名字';
    COMMENT ON COLUMN public.areacode{0}."type" IS '级别,1:省,2:市/州，3区县，4乡镇，5村';
    COMMENT ON COLUMN public.areacode{0}.parent_code IS '父级code';
    '''.format(dataYear)
    return sql

def generateSql(item):
    global sqlSaveIndex
    if sqlSaveIndex == 1:
        writeSql("insert into areacode%s(area_name,code,type,parent_code) values ('%s','%s',%s,'%s')" % (dataYear,item['name'], item['code'], item['type'], item['parentCode']) + ",")
    elif sqlSaveIndex == sqlSaveIndexEnd:
        writeSql("('%s','%s',%s,'%s')" % (item['name'], item['code'], item['type'], item['parentCode']) + ";\n")
        sqlSaveIndex = 0
    else:
        writeSql("('%s','%s',%s,'%s')" % (item['name'], item['code'], item['type'], item['parentCode']) + ",")
    sqlSaveIndex +=1
    
def getItem(itemData, dataArray, parentRequestUrl, table, type):
    item = {}
    # 名称
    if(type == 5):
        item['name'] = str(dataArray[2].get_text())
    else:
        item['name'] = str(dataArray[1].get_text())
    # 下一级请求url
    href = re.findall('(.*)/', parentRequestUrl)
    if type != 5:
        item['url'] = href[0] + "/" + dataArray[0].get('href')
    # 父级code
    item['parentCode'] = itemData.get('code')
    # 类型
    item['type'] = type
    # code码
    item['code'] = str(dataArray[0].get_text())[0:12]
    # if type == 4:
    #     print(item.get('url'))
    # 打印出sql语句
    #print('insert into areacodeinfo(area,code,type,parent_code) values (%s,%s,%s,%s)' % (item['name'], item['code'], item['type'], item['parentCode']) + ";")
    echoinfo(item['name'], item['code'])
    generateSql(item)
    return item

# 获取BeautifulSoup
def getSoup(requestUrl):
    requests.adapters.DEFAULT_RETRIES = 5  # 增加重连次数
    htmls = requests.get(requestUrl, headers=request_headers)
    htmls.encoding = 'utf-8'
    try:
        resultMetaCharset = patternMetaCharset.search(htmls.text)
        if resultMetaCharset:
            pageCharset = resultMetaCharset.group(1).lower()
            rsCharsetIdx = allCharset.index(pageCharset)
            if rsCharsetIdx>0:
                htmls.encoding = resultMetaCharset.group(1)
    except:
        echo('charset is not in default list')
    #soup = BeautifulSoup(htmls.text, 'html.parser', from_encoding='UTF-8')
    #echo(htmls.text)
    soup = BeautifulSoup(htmls.text, 'html.parser')
    return soup

# 循环处理
def forItem(soup, label, labelClass, labelChild, item, requestUrl, type, tableName, lists):
    for link in soup.find_all(label, labelClass):
        array = link.find_all(labelChild, class_='')
        if not len(array):
            continue
        itemData = getItem(item, array, requestUrl, tableName, type)
        lists.append(itemData)
    #time.sleep(2)


# 省列表
def getProvince(provinceList,proviceUrl):
    soup = getSoup(proviceUrl)
    if provinceReg.strip() == '':
        provinceData = soup.find_all('a', class_='')
    else:
        provinceData = soup.find_all(href=re.compile(provinceReg))
    #for link in soup.find_all('a', class_=''):
    #for link in soup.find_all(href=re.compile(provinceReg)):
    for link in provinceData:
        requestCityUrl = re.findall('(.*)/', proviceUrl)
        item = {}
        # 名称
        item['name'] = str(link.get_text())
        # 下一级请求url
        href = str(link.get('href'))
        item['url'] = requestCityUrl[0] + "/" + href
        # 父级code
        item['parentCode'] = '0'
        # 类型
        item['type'] = 1
        # code码
        #item['code'] = (href.split('.'))[0] + '0000000000'
        item['code'] = (href.split('.'))[0]
        provinceList.append(item)
        # 打印出sql语句
        # print('====>',types)
        echoinfo(item['name'],item['code'])
        generateSql(item)
    return provinceList

# 市/州列表
def getCityList(provinceList,cityList):
    for item in provinceList:
        cityRequestUrl = str(item.get('url'))
        soup = getSoup(item.get('url'))
        forItem(soup, 'tr', 'citytr', 'a', item, cityRequestUrl, 2, 'city', cityList)
    #time.sleep(1)
    return cityList
# 区/县列表
def getCountyList(cityList,countyList):
    for item in cityList:
        countyRequestUrl = str(item.get('url'))
        soup = getSoup(item.get('url'))
        forItem(soup, 'tr', 'countytr', 'a', item, countyRequestUrl, 3, 'county', countyList)
    return countyList
# 乡镇列表
def getTownList(countyList,townList):
    for item in countyList:
        townRequestUrl = str(item.get('url'))
        soup = getSoup(item.get('url'))
        forItem(soup, 'tr', 'towntr', 'a', item, townRequestUrl, 4, 'town', townList)
    return townList
# 村庄列表
def getVillageList(townList,villageList):
    for item in townList:
        villageRequestUrl = str(item.get('url'))
        soup = getSoup(item.get('url'))
        forItem(soup, 'tr', 'villagetr', 'td', item,villageRequestUrl, 5, 'village', villageList)
    return villageList

def startSpiders():
    proviceUrl = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/%s/index.html' % dataYear
    baseDir = "data/%s" % dataYear
    #if not os.path.exists('data'):
    #    os.mkdir('data')
    if not os.path.exists(baseDir):
        os.makedirs(baseDir,exist_ok=True)
    provinceList = []
    cityList = []
    countyList = []
    townList = []
    villageList = []
    provinceList = getProvince(provinceList, proviceUrl)
    cityList = getCityList(provinceList, cityList)    
    countyList = getCountyList(cityList, countyList)
    if not simpleData:
        townList = getTownList(countyList, townList)
        getVillageList(townList, villageList)
    # 将最后的，变成；
    replaceLastChar()

def mergeData():
    privinceCodeList = [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43, 44, 45, 46, 50, 51, 52, 53, 54, 61, 62, 63, 64, 65]
    for item in privinceCodeList:
        fileName = "data/%s/areacode%s-%s.sql" % (dataYear,dataYear,item)
        with open(fileName, 'r+') as fo:
            filedata = fo.read(-1)
        if filedata.strip() == '':
            continue
        writeSql(filedata)
        if simpleData and os.path.exists(fileName):
            os.remove(fileName)
    echo("合并完成!")
    echo("清除缓存...","")
    if os.path.exists("cache.data"):
        os.remove("cache.data")
        echo("OK")
    echo("运行结束^_^")

def clearAllContentSaveFile():
    try:
        fp=open(saveFileName,"w+",encoding="utf-8")
        fp.write("")
    finally:
        fp.close()

############### help start ##########################
def _help():
    echo("Usage: %s [Options] [parameter]" % (sys.argv[0]))
    echo("Options:")
    echo("    -?,-h,-help,--help \t :this help")
    echo("    -sql,-c or -sql [year],-c [year] \t :show create databases sql")
    echo("    -y [year] \t :spider all data,Set the parameter dataYear is [year]")
    echo("    -s [year] \t :spider simple data(Only province,city,county),Set the parameter dataYear is [year]")
############### help end ##########################

###########chk is number############
def is_num_by_except(num):
    try:
        int(num)
        return True
    except (ValueError,TypeError):
        print("ERROR: the '%s' is not number" % (num))
        return False
########## function : chk is number ###############
########## function : spidersMain ###############
def spidersMain():
    global saveFileName
    if simpleData:
        saveFileName = "data/areacode%s-simple.sql" % dataYear
    else:
        saveFileName = "data/areacode%s-all.sql" % dataYear
    global provinceReg
    global sqlSaveIndex
    global provinceCodeCache
    #按照省份抓取数据
    privinceCodeList = [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43, 44, 45, 46, 50, 51, 52, 53, 54, 61, 62, 63, 64, 65]
    getProvinceCodeCache()
    for item in privinceCodeList:
        if provinceCodeCache.count(item)>0:
            continue
        saveFileName = "data/%s/areacode%s-%s.sql" % (dataYear,dataYear,item)
        if os.path.exists(saveFileName):
            os.remove(saveFileName)
        provinceReg = '^%s.html' % item
        sqlSaveIndex = 1
        startSpiders()
        time.sleep(5)
        writeProvinceCodeCache(item)
    #合并所有省份数据
    if simpleData:
        saveFileName = "data/areacode%s-simple.sql" % dataYear
    else:
        saveFileName = "data/areacode%s-all.sql" % dataYear
    clearAllContentSaveFile()
    mergeData()

def main():
    global dataYear
    global simpleData
    echo("爬虫开始...")
    argvlen=len(sys.argv)
    if argvlen==1:
        spidersMain()
    else:
        for ii in range(1,argvlen):
            if sys.argv[ii] == '-sql' or sys.argv[ii] == '-c':
                if argvlen==3:
                    if is_num_by_except(sys.argv[ii+1]) and len(sys.argv[ii+1])==4:
                        dataYear = sys.argv[ii+1]
                echo("MySQL create databases SQL is:")
                echo(createTableMySQL())
                echo("PostgreSQL create databases SQL is:")
                echo(createTablePgSQL())
                break
            elif sys.argv[ii] == '-y':
                 if argvlen==3:
                    if is_num_by_except(sys.argv[ii+1]) and len(sys.argv[ii+1])==4:
                        dataYear = sys.argv[ii+1]
                 spidersMain()
                 break
            elif sys.argv[ii] == '-s':
                 if argvlen==3:
                    if is_num_by_except(sys.argv[ii+1]) and len(sys.argv[ii+1])==4:
                        dataYear = sys.argv[ii+1]
                 simpleData = True
                 spidersMain()
                 break
            else:
                _help()

if __name__ == "__main__":
    main()
