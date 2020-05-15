from __future__ import print_function
from flask import Flask, render_template,\
jsonify, request, abort,Response,make_response
from json import dumps
import requests
import json
import datetime
import re
import datetime 
from multiprocessing import Value
from flask_sqlalchemy import SQLAlchemy
import pika
import sys
import uuid
from time import sleep
from random import random
from threading import Timer
import docker
import re
import math
from kazoo.client import KazooClient
from kazoo.client import KazooState
import time

counter = Value('i', 0)

app=Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"]='sqlite:///count.db'

db=SQLAlchemy(app)

slave_counter = Value('i', 1)

class slaves_count_table(db.Model):
	__tablename__='slaves_count_table'
	count=db.Column('count',db.Integer,primary_key=True,default=0)
	def count_request(self):
		return f"slaves_count_table('{self.count}')"

class count_table(db.Model):
	__tablename__='count_table'
	count=db.Column('count',db.Integer,primary_key=True,default=0)
	def count_request(self):
		return f"count_table('{self.count}')"

@app.before_first_request
def tiktok():
	#print("WATCH ON 0000000000")
	zk.exists("/slave_ft/0000000000", watch=spawn_new_slave)
	reset_count()
	def set_interval(func, sec):
		def func_wrapper():
			set_interval(func, sec)
			func()
		t=Timer(sec, func_wrapper)
		t.start()
		return t
	set_interval(reset_count,120)

def checkpathcontains(path):
	if(path.find('/api/v1/db/read')!=-1):
		return 1
	return 0

def increm_slave_count():
	result=slaves_count_table.query.filter_by()
	for r in result:
		with slave_counter.get_lock():		
			db.create_all()		
			total_count=int(r.count)+int(1)
			db.session.delete(r)	
			db.session.commit()	
			entry=slaves_count_table(count=int(total_count))
			db.session.add(entry)
			db.session.commit()

def decrem_slave_count():
	result=slaves_count_table.query.filter_by()
	for r in result:
		with slave_counter.get_lock():		
			db.create_all()		
			total_count=int(r.count)-int(1)
			db.session.delete(r)	
			db.session.commit()	
			entry=slaves_count_table(count=int(total_count))
			db.session.add(entry)
			db.session.commit()
def spawn_new_slave(event):
	client = docker.from_env()
	conts = client.containers.list(all=True)
	a=[]
	for i in conts:
		name=i.name
		#print("NAME OF WORKER :", name)
		if(name.find("worker")!=-1):
			a.append(int(name[6:]))
	#print(" THIS IS A :" ,a)	
	if(not a):
		maxSlaveIndex=0
	else:
		maxSlaveIndex=max(a)
	client.containers.run("worker:latest",detach=True,network='cont_net',links={"rabbitmq":"rabbitmq1"},volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}},name="worker"+str(maxSlaveIndex+1), pid_mode='host')

def scale_out(req_slaves, slav_count):
	client = docker.from_env()
#	if(slav_count==):
#		client.images.build(path="./", dockerfile="Dockerfile", tag="worker:latest", forcerm=True)
	i=0
	while(req_slaves):
		increm_slave_count()
		conts = client.containers.list(all=True)
		a=[]
		for i in conts:
			name=i.name
			#print("NAME OF WORKER :", name)
			if(name.find("worker")!=-1):
				a.append(int(name[6:]))
		#print(" THIS IS A :" ,a)	
		maxSlaveIndex=max(a)
		container = client.containers.run("worker:latest",detach=True,network='cont_net',links={"rabbitmq":"rabbitmq1"},volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}},name="worker"+str(maxSlaveIndex+1), pid_mode='host')
		req_slaves=req_slaves-1
	return

def scale_in(req_slaves, slav_count):
	client = docker.from_env()
	conts = client.containers.list(all=True)
	while(req_slaves):
		decrem_slave_count()
		conts = client.containers.list(all=True)
		#print("NAME OF CONT TO BE RESTARTED: ", conts[0].name)
		for i in conts:
			name=i.name
			if(name.find("worker")!=-1):
				i.remove(force=True)
				req_slaves=req_slaves-1
				break
	return

@app.route('/api/v1/crash/slave',methods=["POST"])
def crash_slave():
	client = docker.from_env()
	conts = client.containers.list(all=True)
	pids = []
	maxPid = -99999
	Id=0
	restCont=0
	cli = docker.APIClient(base_url='unix:///var/run/docker.sock')
	masternodedata,stat=zk.get("/election/master")
	masternodedata=masternodedata[2:]
	masternodedata=masternodedata[0:len(masternodedata)-1]
	children3=[]
	children = zk.get_children("/slave_ft")
	for t in children:
		data, stat = zk.get("/slave_ft/%s"%t)
		data=data.decode('utf8')
		children3.append((t,int(data)))

	for i in conts:
		cont=cli.inspect_container(i.name)
		name=i.name
		if(name.find("worker")!=-1 and cont['State']['Pid'] > maxPid and cont['State']['Pid']!=int(masternodedata)):
			maxPid = cont['State']['Pid']
			restCont = i
	for t in children3:
		if(t[1]==maxPid):
			#print("WATCH ON %s and Children are %s"%(t[0], children))
			zk.exists("/slave_ft/%s"%t[0], watch=spawn_new_slave)
			break
	restCont.remove(force=True)
	return make_response(dumps([maxPid]))

@app.route('/api/v1/crash/master',methods=["POST"])
def crash_master():
	global zk
	client = docker.from_env()
	conts = client.containers.list(all=True)
	cli = docker.APIClient(base_url='unix:///var/run/docker.sock')
	masternodedata,stat=zk.get("/election/master")
	masternodedata=masternodedata[2:]
	masternodedata=masternodedata[0:len(masternodedata)-1]
	#print("MASTER NODE DATA : ", masternodedata)
	children=zk.get_children("/election")
	children2=[]
	for t in children:
		data,stat=zk.get("/election/%s"%t)
		if(t!='master'):
			children2.append((t,int(data)))
	#print("Children2 LIST", children2)
	masterpid=0
	masterid=0
	for t in children2:
		if(t[1]==int(masternodedata)):
			#print("FOUND IT")
			masterpid=t[1]
			masterid=t[0]
			break
	#print("AFTER FOUND IT")
	for i in conts:
		cont=cli.inspect_container(i.name)
		if(cont['State']['Pid']==masterpid):
			
			a=[]
			for j in conts:
				name=j.name
				if(name.find("worker")!=-1):
					a.append(int(name[6:]))
			maxSlaveIndex=max(a)
			i.remove(force=True)
			client.containers.run("worker:latest",detach=True,network='cont_net',volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'}},links={"rabbitmq":"rabbitmq1"},name="worker"+str(maxSlaveIndex+1), pid_mode='host')
			break

	return make_response(dumps([masterpid]))
	
@app.route('/api/v1/worker/list',methods=["GET"])
def list_workers():
	client = docker.from_env()
	cli = docker.APIClient(base_url='unix:///var/run/docker.sock')
	conts = client.containers.list(all=True)
	pids = []
	for i in conts:
		name=i.name
		if(name.find("master")!=-1 or name.find("worker")!=-1):
			cont=cli.inspect_container(i.name)
			#print("PIDS: ",cont['State']['Pid'])
			pids.append(cont['State']['Pid'])
	pids.sort()
	return jsonify(pids)
		


@app.before_request
def before_request_func():
	
	path = request.path
	bul=checkpathcontains(path)
	if(bul):
		#print("MOFO")
		result=count_table.query.filter_by()
		for r in result:
			#print("RES",r)
			with counter.get_lock():		
				db.create_all()
				
				total_count=int(r.count)+int(1)
				#print("Read API Counts: ", total_count)
				db.session.delete(r)	
				db.session.commit()	
				entry=count_table(count=int(total_count))
				db.session.add(entry)
				db.session.commit()



class write_object(object):

	def __init__(self):
		#print("BEGINING MFO")
		self.credential_params = pika.PlainCredentials('guest', 'guest')
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials = self.credential_params))

		self.channel = self.connection.channel()
		self.channel.queue_declare(queue='write_response_queue',durable=True)
		self.callback_queue ='write_response_queue'
		
		#print("INSIDE MOTHERFUCKER")
		self.channel.basic_consume(
		queue=self.callback_queue,
		on_message_callback=self.on_response,auto_ack=True)

	def on_response(self, ch, method, props, body):
				
		if self.corr_id == props.correlation_id:
			self.response = body
	
	def call(self,send_data):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		send_data=json.dumps(send_data)
		
		self.channel.basic_publish(
		    exchange='orchestrator_exchange',
		    routing_key='write',
		    properties=pika.BasicProperties(
		        reply_to=self.callback_queue,
		        correlation_id=self.corr_id,
		    ),
		    body=send_data)
		while self.response is None:
			
			self.connection.process_data_events()
		#print("AFTER WHILE BIYOCH")
		self.connection.close()
		return self.response

class read_object(object):

	def __init__(self):
		self.credential_params = pika.PlainCredentials('guest', 'guest')
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials = self.credential_params))

		self.channel = self.connection.channel()

		self.channel.queue_declare(queue='read_response_queue',durable=True)
		self.callback_queue ='read_response_queue'

		self.channel.basic_consume(
		queue=self.callback_queue,
		on_message_callback=self.on_response,auto_ack=True)

	def on_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body
	
	def call(self,send_data):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		send_data=json.dumps(send_data)
		
		self.channel.basic_publish(
		    exchange='orchestrator_exchange',
		    routing_key='read',
		    properties=pika.BasicProperties(
		        reply_to=self.callback_queue,
		        correlation_id=self.corr_id,
		    ),
		    body=send_data)
		
		while self.response is None:
			#print("in while")
			self.connection.process_data_events()
			#print("in while2")
		self.connection.close()

		return self.response



# API 8 write db
@app.route('/api/v1/db/write',methods=["PUT","DELETE","POST"])
def write_db():
	send_data=request.get_json()
	send_object=write_object()
	resp=send_object.call(json.dumps(send_data))
	resp=json.loads(resp)	
	return  str(resp)


# API 9 read db
@app.route('/api/v1/db/read',methods=["POST"])
def read_db():
	send_data=request.get_json()
	send_object=read_object()	
	resp=send_object.call(json.dumps(send_data))
	resp=json.loads(resp)
	return  jsonify(resp)

def reset_count():
	result=count_table.query.filter_by()
	r_count=0
	for r in result:
		r_count=r.count
		break
	result=slaves_count_table.query.filter_by()
	slave_count=1
	for slave in result:
		slave_count=slave.count
		break
	no_slaves = math.ceil(r_count/20)
	if(no_slaves==0):
		no_slaves=1		
	req_slaves = no_slaves - slave_count
	if(req_slaves > 0):
		scale_out(req_slaves, slave_count)
	elif((req_slaves < 0) and (slave_count > 1) ):
		ppp=1
		#scale_in(-1*req_slaves, slave_count)
	with counter.get_lock():
		counter.value=0
	result=count_table.query.filter_by()
	for r in result:
		with counter.get_lock():	
			db.session.delete(r)	
			db.session.commit()		
			entry=count_table(count=int(0))
			db.create_all()
			db.session.add(entry)
			db.session.commit()
	response = Response(response=json.dumps(dict()),status=200, mimetype='application/json')
	return response

def my_listener(state):
	if(state == KazooState.LOST):
		xafsaf=0		
	elif(state == KazooState.SUSPENDED):
		afafax=0
	else:
		xafaf=0

if __name__ == "__main__":
	zk = KazooClient(hosts='zoo:2181')
	zk.add_listener(my_listener)
	zk.start()
	if not zk.exists("/election"):
		zk.create("/election", b"Welcome to the World of Slave Trade!")
		#print("Welcome to the World of Slave Trade!")
	if not zk.exists("/slave_ft"):
		zk.create("/slave_ft", b"Slave Fault Tolerance")
	
	db.create_all()
	with counter.get_lock():
		counter.value=0
	result=count_table.query.filter_by()
	for r in result:
		with counter.get_lock():	
			db.session.delete(r)	
			db.session.commit()
	entry=count_table(count=int(0))
	db.session.add(entry)
	db.session.commit()
	
	with slave_counter.get_lock():
		slave_counter.value=1

	result=slaves_count_table.query.filter_by()
	for r in result:
		with slave_counter.get_lock():	
			db.session.delete(r)	
			db.session.commit()		
	entry=slaves_count_table(count=int(1))
	db.session.add(entry)
	db.session.commit()
	app.run(debug = True, host = '0.0.0.0', port =80)


