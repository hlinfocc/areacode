#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import sys
import os
import re
from bs4 import BeautifulSoup
import string

#设置请求头
request_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Cookie": "_trs_uv=jz3i785b_6_2zxi; AD_RS_COOKIE=20088745",
    "Host": "www.stats.gov.cn",
    "DNT": "1",
    "If-Modified-Since": "Thu, 10 Sep 2020 05:53:29 GMT",
    "If-None-Match": "1c98-580baa54b4840-gzip",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0"
}

#insert语句的索引，当达到指定值后重新生成insert
sqlSaveIndex = 1
#当一条insert 的 values达到该值后重新生成新的一条insert
sqlSaveIndexEnd = 10000
#保存的文件名
saveFileName = "data/areacode2020.sql"

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
            CREATE TABLE IF NOT EXISTS areacode2020 (
            code  varchar(20) PRIMARY KEY NOT NULL COMMENT '地址code',
            area_name  varchar(255) DEFAULT '' COMMENT '名字',
            type  int COMMENT '级别,1:省,2:市/州，3区县，4乡镇，5村',
            parent_code varchar(20) COMMENT '父级code ',
            KEY `areacode_index` (`parent_code`)
            ) DEFAULT CHARSET=utf8 COMMENT='地址表2020';\n
    '''
    return create_tb_cmd

def createTablePgSQL():
    sql = '''
    CREATE TABLE if not exists public.areacode2020 (
        code varchar(20) NULL,
        area_name text NULL,
        "type" integer NULL,
        parent_code varchar(20) NULL,
        CONSTRAINT areacode2020_pk PRIMARY KEY (code)
    );
    CREATE INDEX areacode2020_parent_code_idx ON public.areacode2020 (parent_code);
    CREATE INDEX areacode2020_type_idx ON public.areacode2020 ("type");
    COMMENT ON TABLE public.areacode2020 IS '地址表2020';
    COMMENT ON COLUMN public.areacode2020.code IS '地址code';
    COMMENT ON COLUMN public.areacode2020.area_name IS '名字';
    COMMENT ON COLUMN public.areacode2020."type" IS '级别,1:省,2:市/州，3区县，4乡镇，5村';
    COMMENT ON COLUMN public.areacode2020.parent_code IS '父级code';
    '''
    return sql
def getItem(itemData, dataArray, parentRequestUrl, table, type):
    global sqlSaveIndex
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
    if sqlSaveIndex == 1:
        writeSql("insert into areacode2020(area_name,code,type,parent_code) values ('%s','%s',%s,'%s')" % (item['name'], item['code'], item['type'], item['parentCode']) + ",")
    elif sqlSaveIndex == sqlSaveIndexEnd:
        writeSql("('%s','%s',%s,'%s')" % (item['name'], item['code'], item['type'], item['parentCode']) + ";\n")
        sqlSaveIndex = 0
    else:
        writeSql("('%s','%s',%s,'%s')" % (item['name'], item['code'], item['type'], item['parentCode']) + ",")
    sqlSaveIndex +=1
    return item

# 获取BeautifulSoup
def getSoup(requestUrl):
    htmls = requests.get(requestUrl, headers=request_headers)
    htmls.encoding = 'GBK'
    #soup = BeautifulSoup(htmls.text, 'html.parser', from_encoding='UTF-8')
    echo(htmls.text)
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


# 省列表
def getProvince(provinceList,proviceUrl):
    soup = getSoup(proviceUrl)
    for link in soup.find_all('a', class_=''):
    #for link in soup.find_all(href=re.compile('^52.html')):
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
        writeSql("insert into areacode2020(area_name,code,type,parent_code) values ('%s','%s',%s,'%s')" % ((item['name']), item['code'], item['type'], item['parentCode']) + ";\n")
        echoinfo(item['name'],item['code'])
    return provinceList

# 市/州列表
def getCityList(provinceList,cityList):
    for item in provinceList:
        cityRequestUrl = str(item.get('url'))
        soup = getSoup(item.get('url'))
        forItem(soup, 'tr', 'citytr', 'a', item, cityRequestUrl, 2, 'city', cityList)
    return cityList
# 区/县列表
def getCountyList(cityList,countyList):
    for item in cityList:
        countyRequestUrl = str(item.get('url'))
        soup = getSoup(item.get('url'))
        forItem(soup, 'tr', 'countytr', 'a', item, countyRequestUrl, 3, 'county', countyList)
    return countyList
# 城镇列表
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


def main():
    proviceUrl = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2020/index.html'
    if not os.path.exists('data'):
        os.mkdir('data')
    provinceList = []
    cityList = []
    countyList = []
    townList = []
    villageList = []
    provinceList = getProvince(provinceList,proviceUrl)
    #cityList = getCityList(provinceList,cityList)
    #countyList = getCountyList(cityList,countyList)
    #townList = getTownList(countyList,townList)
    #getVillageList(townList,villageList)
    #将最后的，变成；
    #replaceLastChar()

if __name__ == "__main__":
    main()
