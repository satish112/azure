import os
from flask import Flask,redirect,render_template,request
import pypyodbc
import time
import random
import urllib
import datetime
import json
import redis
import pickle
import hashlib

app = Flask(__name__)


server = 'sham05.database.windows.net'
database = 'sqldb'
username = 'sham05'
password = '1qaz!QAZ'
driver= '{ODBC Driver 13 for SQL Server}'
myHostname = "shamuta.redis.cache.windows.net"
myPassword = "qV08ERHDwOaSAn0WMo4ZwUn9AK34bMNIAziA2YIqQbk="

r = redis.Redis(host='shamuta.redis.cache.windows.net',
        port=6379, db=0, password='qV08ERHDwOaSAn0WMo4ZwUn9AK34bMNIAziA2YIqQbk=')
   
def randrange(rangfro=None,rangto=None,num=None):
    dbconn = pypyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = dbconn.cursor()
    start = time.time()
    for i in range(0,int(num)):
        mag= round(random.uniform(rangfro, rangto),1)
        success="SELECT * from [earth_data] where mag>'"+str(mag)+"'"
        hash = hashlib.sha224(success.encode('utf-8')).hexdigest()
        key = "redis_cache:" + hash
        if (r.get(key)):
           print("redis cached")
        else:
           # Do MySQL query   
           cursor.execute(success)
           data = cursor.fetchall()
           rows = []
           for j in data:
                rows.append(str(j))  
           # Put data into cache for 1 hour
           r.set(key, pickle.dumps(list(rows)) )
           r.expire(key, 36);
        cursor.execute(success)
    end = time.time()
    exectime = end - start
    return render_template('count.html', t=exectime)
	
def disdata(rangfro=None,rangto=None,num=None):
    dbconn = pypyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = dbconn.cursor()
    start = time.time()
    for i in range(0,int(num)):
        mag= round(random.uniform(rangfro, rangto),1)
        success="SELECT * from [earth_data] where mag>'"+str(mag)+"'"
        cursor.execute(success)
    end = time.time()
    exectime = end - start
    return render_template('count.html', t=exectime)

@app.route('/')
def hello_world():
  return render_template('index.html')

@app.route('/displaydata', methods=['GET'])
def display():
    rangfro = float(request.args.get('rangefrom1'))
    rangto = float(request.args.get('rangeto1'))
    num = request.args.get('nom1')
    return disdata(rangfro,rangto,num) 

@app.route('/multiplerun', methods=['GET'])
def randquery():
    rangfro = float(request.args.get('rangefrom'))
    rangto = float(request.args.get('rangeto'))
    num = request.args.get('nom')
    return randrange(rangfro,rangto,num) 	

if __name__ == '__main__':
  app.run()
