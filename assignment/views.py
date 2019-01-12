from django.shortcuts import render
import os
import re
import csv, io, json
from django.http import JsonResponse
import pandas as df
from datetime import datetime, timedelta
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Timer, Thread, Event

# Create your views here.

class TransactionList():

    def __init__(self):
        self.transactionsList = []
        self.dFrameTransaction = df.DataFrame(self.transactionsList)

    def initDatabaseForTransaction(self):
        path = "./"

        transactionsList = []

        for fname in os.listdir(path):

            if re.match(r"^Transaction_*", fname):
                with open(os.path.join(path, fname), 'r') as f:
                    csvReader = csv.DictReader(f)
                    for row in csvReader:
                        transactionObj = dict(row)
                        transactionObj = {
                            'productId' : int(transactionObj['productId']),
                            'transactionId': int(transactionObj['transactionId']),
                            'transactionAmount':float(transactionObj['transactionAmount']),
                            'transactionDatetime': df.to_datetime(transactionObj['transactionDatetime'])
                        }
                        transactionsList.append(transactionObj)
                print(fname)

        self.transactionsList = transactionsList

        self.dFrameTransaction = df.DataFrame(self.transactionsList)

        return self.transactionsList, self.dFrameTransaction


    def getTransaction(self, id):
        queryData = self.dFrameTransaction[(self.dFrameTransaction['transactionId'] == id)]
        transactionJson = json.loads(queryData.iloc[0].to_json())

        transactionJson['transactionDatetime'] = df.to_datetime(transactionJson['transactionDatetime'] / 1000.0,
                                                                unit='s') \
            .strftime("%Y-%m-%d %H:%M:%S")

        transactionJson['productName'] = productList.getProductNameById(queryData.iloc[0].productId)

        return transactionJson


    def getSummaryByCity(self, lastndays):
        queryData = self.dFrameTransaction[
            (self.dFrameTransaction['transactionDatetime'] > df.to_datetime(
                datetime.today() - timedelta(days=lastndays))) &
            (self.dFrameTransaction['transactionDatetime'] < df.to_datetime(datetime.now()))].groupby(
            'productId').sum()

        summaryByManufacturingCityList = []

        for index, row in queryData.iterrows():
            summaryByManufacturingCityListObj = {
                'cityName': productList.getProductCityById(index),
                'totalAmount': float(row['transactionAmount'])
            }
            summaryByManufacturingCityList.append(summaryByManufacturingCityListObj)

        return summaryByManufacturingCityList


    def getSummaryByProduct(self, lastndays):
        queryResp = self.dFrameTransaction[
            (self.dFrameTransaction['transactionDatetime'] > df.to_datetime(
                datetime.today() - timedelta(days=lastndays))) &
            (self.dFrameTransaction['transactionDatetime'] < df.to_datetime(datetime.now()))].groupby(
            'productId').sum()

        summaryByProductsList = []

        for index, row in queryResp.iterrows():
            summaryByProductsListObj = {
                'productName': productList.getProductNameById(index),
                'totalAmount': float(row['transactionAmount'])
            }
            summaryByProductsList.append(summaryByProductsListObj)

        return summaryByProductsList

transactionList = TransactionList()
transactionList.initDatabaseForTransaction()

class WatchdogHandler(FileSystemEventHandler):
    def on_modified(self, event):
        print ('modifiedd')
        transactionList.initDatabaseForTransaction()

    def dispatch(self, event):
        print (event)
        transactionList.initDatabaseForTransaction()


class ProductList(object):
    def __init__(self):
        self.productsList = []
        self.dFrameProducts = df.DataFrame(self.productsList)

    def initDatabaseForProduct(self):
        path = "./"

        productList = []

        for fname in os.listdir(path):
            if fname == "ProductReference.csv":
                with open(os.path.join(path, fname), 'r') as f:
                    csvReader = csv.DictReader(f)
                    for row in csvReader:
                        productObj = dict(row)
                        productObj = {
                            'productId': int(productObj['productId']),
                            'productName': productObj['productName'],
                            'productManufacturingCity': productObj['productManufacturingCity']
                        }
                        productList.append(productObj)
                print(fname)

        self.productsList = productList
        self.dFrameProducts = df.DataFrame(self.productsList)

        return self.dFrameProducts

    def getProductNameById(self, productId):
        productDetails = self.dFrameProducts[(self.dFrameProducts['productId'] == productId)]
        productDetails = json.loads(productDetails.iloc[0].to_json())
        return productDetails['productName']

    def getProductCityById(self, productId):
        productDetails = self.dFrameProducts[(self.dFrameProducts['productId'] == productId)]
        productDetails = json.loads(productDetails.iloc[0].to_json())
        return productDetails['productManufacturingCity']


productList = ProductList()
productsDataFrame = productList.initDatabaseForProduct()


class PT(Thread):

    def __init__(self):
        Thread.__init__(self)

    def run(self):
        event_handler = WatchdogHandler()
        observer = Observer()
        observer.schedule(event_handler, path='./', recursive=False)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

t = PT()
t.start()


def getProductManufacturingCity(productId):
    productDetails = productsDataFrame[(productsDataFrame['productId'] == productId)]
    productDetails = json.loads(productDetails.iloc[0].to_json())
    return productDetails['productManufacturingCity']


def transactionSummaryByManufacturingCity(request, lastndays):
    summaryByManufacturingCityList = transactionList.getSummaryByCity(lastndays)
    return JsonResponse({'summary': summaryByManufacturingCityList})



def transactionSummaryByProducts(request, lastndays):
    summaryByProductsList = transactionList.getSummaryByProduct(lastndays)
    return JsonResponse({'summary': summaryByProductsList})


def transaction(request, transactionId):
    transactionJson = transactionList.getTransaction(transactionId)
    return JsonResponse(transactionJson)
