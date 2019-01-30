#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 19:19:36 2018

@author: Chunyan
"""

from urllib.request import urlopen
import re
from datetime import datetime
import time
import mysql.connector
import _thread
from queue import Queue

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime


import bs4
from bs4 import BeautifulSoup as soup
import requests

hosturl 	= "34.196.73.16"
dbuser 		= "root"
dbpassoword = "password"
dbname 		= "modelStockprediction"

stock_table_name = ['tbl_align', 'tbl_poly', 'tbl_aal', 'tbl_ibm', 'tbl_rrs']
stockNames = ['ALGN', 'POLY', 'AAL', 'IBM', 'RRS']

prev_market_type = ['OPEN', 'OPEN', 'OPEN', 'OPEN', 'OPEN']

tradingview_url = 'https://www.tradingview.com/symbols/'
stock_url = ['NASDAQ-ALGN', 'LSE-POLY', 'LSE-AAL', 'SIX-IBM', 'TSXV-RRS']

tv_url='https://www.marketwatch.com/investing/stock/'
stockUrl = ['algn', 'poly?countrycode=uk', 'aal?countrycode=uk', 'ibm', 'rrs?countrycode=ca']

# prev_market_type = ['OPEN', 'OPEN'. 'OPEN', 'OPEN', 'OPEN']

path_to_chromedriver = '/usr/bin/chromedriver'

options = webdriver.ChromeOptions()
prefs = {"profile.default_content_setting_values.notifications" : 2}
# options.add_experimental_option("prefs",prefs)
options.add_argument('headless')
# options.add_argument('window-size=1200,1100')
options.add_argument("disable-infobars")
# options.add_argument("--incognito");
options.add_argument('--disable-gpu')
options.add_argument("--disable-extensions")

options.add_argument("--disable-impl-side-painting")
options.add_argument("--disable-accelerated-2d-canvas'")
options.add_argument("--disable-gpu-sandbox")
options.add_argument("--no-sandbox")
options.add_argument("--disable-extensions")
options.add_argument("--dns-prefetch-disable")

def checkState(datas):
    if(datas.upper() == 'CLOSED' or datas.upper() == 'AFTER HOURS'):
        return 1
    else:
        return 0


def StockDataToSql(data1, data2, data3):
    try:
        mydb = mysql.connector.connect(
              host      = hosturl,
              user      = dbuser,
              passwd    = dbpassoword,
              database  = dbname
        )
        mycursor = mydb.cursor()

        for i in range(5):
            if((prev_market_type[i] != data1[i][1]) and !(checkState(prev_market_type[i]) == 1 and checkState(data1[i][1]) ==1)):

                datastr = ""

                for j in range(29):                                    
                    datastr = datastr + "'" + data1[i][j] + "'" + ","               
            
                for j in range(11):
                    datastr = datastr + "'" + data2[i][j] + "'" + ","

                datastr = datastr + "'" + data3[i][0] + "'" + ","
                datastr = datastr + "'" + data3[i][1] + "'" + ","

                mydatetime = datetime.datetime.now().strftime("%y-%m-%d %H:%M")
                datastr = datastr + "'" + mydatetime + "'"

                datastr = "(" + datastr + ")"

                sql = "INSERT INTO " + stock_table_name[i] + " (symbolName, marketType, price, changeValue, changePercent, open, marketCap, sharesOutstanding, publicFloat, beta, revPerEmployee, peRatio, eps, yield, dividend, exdividendDate, shortInterest, floatShorted, averageVolume, dayLow, dayHigh, weekLow52, weekHigh52, week1, month1, month3, ytd, year1, volume, PricetoBookRatio, QuickRatio, CurrentRatio, DERatio, ReturnonAssets, ReturnonEquity, ReturnonInvestedCapital, NetMargin, GrossMargin, OperatingMargin, PreTaxMargin, Recommendations, TargetPrice, GotTime) VALUES " + datastr

                mycursor.execute(sql)
                mydb.commit()

                print('saved', prev_market_type[i], data1[i][1], i)

            else:
                print('unsaved')

            prev_market_type[i] = data1[i][1]
            
        mycursor.close()
        mydb.close()
        print("StockData Saved Successfully!")
    except Exception as e: print(e)

def getStockData():
    rtArray =  list()

    for i in range(5):
        geturl = tv_url+stockUrl[i]

        driver = webdriver.Chrome(executable_path = path_to_chromedriver, chrome_options=options)
        driver.get(geturl)

        while True:
            try:
                Create = driver.find_element_by_xpath("//ul[@class='tabs']/li[2]/a")
                driver.execute_script("arguments[0].click()", Create)                
                break
            except TimeoutException :
                print("Retrying...")
                continue
        
        res = driver.execute_script("return document.documentElement.outerHTML")
        driver.quit()

        page_soup = soup(res, "lxml")
        containers = page_soup.findAll("div", {"class":"stock"})[0]
        status = containers.findAll("small", {"class":"intraday__status"})

        text = status[0].text;

        p=status[0].findAll("span",{"class":"company__ticker"})[0].text
        text=text.replace(p,"")
        p=status[0].findAll("span",{"class":"company__market"})[0].text
        text=text.replace(p,"")
        p=status[0].findAll("span",{"class":"scroll-top"})[0].text
        text=text.replace(p,"")

        marketType = text

        pp = containers.findAll("bg-quote", {"class":"value"})
        price = ""
        changeValue = ""
        changePercent = ""
        if(pp == []):
            pp = containers.findAll("span", {"class":"value"})
            price = pp[0].text

            pp = containers.findAll("span", {"class":"change--point--q"})
            changeValue = pp[0].text

            pp = containers.findAll("span", {"class":"change--percent--q"})
            changePercent = pp[0].text
        else:
            price = pp[0].text

            pp = containers.findAll("bg-quote", {"field":"change"})
            changeValue = pp[0].text

            pp = containers.findAll("bg-quote", {"field":"percentchange"})
            changePercent = pp[0].text

        pp = containers.findAll("td",{"class":"u-semi"})
        preClose = pp[0].text

        pp = containers.findAll("span", {"class":"last-value"})
        volume = pp[0].text.strip()

        pp = containers.findAll("mw-rangebar", {"class":"lowHigh--day"})[0]
        qq = pp.findAll("span", {"class":"low"})
        dayLow = qq[0].text

        pp = containers.findAll("mw-rangebar", {"class":"lowHigh--day"})[0]
        qq = pp.findAll("span", {"class":"high"})
        dayHigh = qq[0].text

        pp = containers.findAll("mw-rangebar", {"class":"lowHigh--year"})[0]
        qq = pp.findAll("span", {"class":"low"})
        weekLow52 = qq[0].text

        pp = containers.findAll("mw-rangebar", {"class":"lowHigh--year"})[0]
        qq = pp.findAll("span", {"class":"high"})
        weekHigh52 = qq[0].text

        pp = containers.findAll("li", {"class":"kv__item"})

        qq = pp[0].findAll("span",{"class":"kv__primary"})
        Open = qq[0].text

        qq = pp[3].findAll("span",{"class":"kv__primary"})
        marketCap = qq[0].text

        qq = pp[4].findAll("span",{"class":"kv__primary"})
        sharesOutstanding = qq[0].text
        
        qq = pp[5].findAll("span",{"class":"kv__primary"})
        publicFloat = qq[0].text
        
        qq = pp[6].findAll("span",{"class":"kv__primary"})
        beta = qq[0].text
        
        qq = pp[7].findAll("span",{"class":"kv__primary"})
        revPerEmployee = qq[0].text
        
        qq = pp[8].findAll("span",{"class":"kv__primary"})
        peRatio = qq[0].text
        
        qq = pp[9].findAll("span",{"class":"kv__primary"})
        eps = qq[0].text
        
        qq = pp[10].findAll("span",{"class":"kv__primary"})
        Yield = qq[0].text
        
        qq = pp[11].findAll("span",{"class":"kv__primary"})
        dividend = qq[0].text
        
        qq = pp[12].findAll("span",{"class":"kv__primary"})
        exDividendDate = qq[0].text
        
        qq = pp[13].findAll("span",{"class":"kv__primary"})
        shortInterest = qq[0].text
        
        qq = pp[14].findAll("span",{"class":"kv__primary"})
        floatShorted = qq[0].text
        
        qq = pp[15].findAll("span",{"class":"kv__primary"})
        averageVolume = qq[0].text


        #PERFORMANCE
        pp = containers.findAll("li", {"class":"ignore-color"})
        week1 = pp[0].text
        month1 = pp[1].text
        month3 = pp[2].text
        ytd = pp[3].text
        year1 = pp[4].text

        realtimeData = [
            stockNames[i],         
            marketType,
            price,
            changeValue,
            changePercent,
            Open,
            marketCap,
            sharesOutstanding,
            publicFloat,
            beta,
            revPerEmployee,
            peRatio,
            eps,
            Yield,
            dividend,
            exDividendDate,
            shortInterest,
            floatShorted,
            averageVolume,
            dayLow,
            dayHigh,
            weekLow52,
            weekHigh52,
            week1,
            month1,
            month3,
            ytd,
            year1,
            volume
        ]

        rtArray.append(realtimeData)

        time.sleep(1)
        
    return rtArray

def getStockData2():
    rtArray = list()

    for i in range(5):
        try:
            MarketCapitalization = ""

            new_url = tradingview_url + stock_url[i]

            driver = webdriver.Chrome(executable_path = path_to_chromedriver, chrome_options=options) #load chrome driver
            driver.get(new_url)
            res = driver.execute_script("return document.documentElement.outerHTML")
            driver.quit()

            page_soup = soup(res, "lxml")

            containers = page_soup.findAll("div", {"class":"tv-feed-widget--fundamentals"})
            all_value = containers[0].findAll("span", {"class":"tv-widget-fundamentals__value"})
            
            PricetoBookRatio = all_value[8].text.strip()
            QuickRatio = all_value[14].text.strip()
            CurrentRatio = all_value[15].text.strip()
            DERatio = all_value[16].text.strip()
            ReturnonAssets = all_value[24].text.strip()
            ReturnonEquity = all_value[25].text.strip()
            ReturnonInvestedCapital = all_value[26].text.strip()
            NetMargin = all_value[28].text.strip()
            GrossMargin = all_value[29].text.strip()
            OperatingMargin = all_value[30].text.strip()
            PreTaxMargin = all_value[31].text.strip()
            
            realtimeData = [
                PricetoBookRatio,
                QuickRatio,
                CurrentRatio,
                DERatio,
                ReturnonAssets,
                ReturnonEquity,
                ReturnonInvestedCapital,
                NetMargin,
                GrossMargin,
                OperatingMargin,
                PreTaxMargin
            ]
            rtArray.append(realtimeData)
            time.sleep(1)

        except Exception as ex:
            realtimeData = [
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                ''
            ]
            rtArray.append(realtimeData)
            time.sleep(1)
            pass
        # rtArray.append(realtimeData)
    return rtArray

def getStockData3():
    rtArray = list()

    for i in range(5):
        driver = webdriver.Chrome(path_to_chromedriver, chrome_options=options)

        url = tv_url + stockUrl[i] +"/analystestimates"
        driver.get(url)

        res = driver.execute_script("return document.documentElement.outerHTML")
        driver.quit()

        page_soup = soup(res, "lxml")

        containers = page_soup.findAll("table", {"class":"snapshot"})

        if(containers == []):
            Recommendations = ""
            TargetPrice = ''
        else:
            cont = containers[0].findAll("td")
            Recommendations = cont[1].text.strip()
            TargetPrice = cont[3].text.strip()

        realtimeData = [
            Recommendations,
            TargetPrice
        ]
        rtArray.append(realtimeData)
        time.sleep(1)

    return rtArray


def main():
    while 1:
        start_time = time.time()
        data1 = getStockData()
        data2 = getStockData2()
        data3 = getStockData3()
        StockDataToSql(data1, data2, data3)
        execute_time = time.time() - start_time
        print("Stock Scrapping Time:", execute_time)
        time.sleep(3600 - execute_time)
main()
