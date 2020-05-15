from sqlalchemy.ext.declarative  import declarative_base
from sqlalchemy import Column,Integer,String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from json import dumps
import json
import datetime
import re
import datetime 
from flask import jsonify
import pika
import time
import atexit
import os
import subprocess
import uuid
from kazoo.client import KazooClient
from kazoo.client import KazooState
from threading import Lock
from threading import Thread
import docker
import requests

lock = Lock()
sync_queue=None
channel_sync=None
Base=declarative_base()
engine = create_engine('sqlite:///user_ride.db', echo=True)
Session = sessionmaker(bind=engine)

class user_table(Base):
	__tablename__='user_table'
	username=Column('username',String(40),primary_key=True)
	pwd=Column('pwd',String(40),primary_key=True)
	
	def __repr__(self):
		return f"user_table('{self.username}')"

class ride_table(Base):
	__tablename__='ride_table'
	
	ride_id=Column('ride_id',Integer,primary_key=True)
	source=Column('source',Integer)
	destination=Column('destination',Integer)
	creator=Column('creator',String(40))
	create_time=Column('create_time',String(40))
	
	def ride_table(self):
		return f"ride_table('{self.ride_id}','{self.source}','{self.destination}','{self.creator}','{self.create_time}')"

class riders_table(Base):
	__tablename__='riders_table'
	ride_id=Column('ride_id',Integer,primary_key=True)
	rider=Column('rider',String(40),primary_key=True)
	def riders_table(self):
		return f"riders_table('{self.ride_id}','{self.rider}')"

class ride_id_table(Base):
	__tablename__='ride_id_table'
	ride_id=Column('ride_id',Integer,primary_key=True,default=0)
	def count_request(self):
		return f"ride_id_table('{self.ride_id}')"



Base.metadata.create_all(bind=engine)

		
#write_function write_db

def write_function(ch, method, props, body,flag):
	global sync_queue
	global channel_sync
	global Session
	global lock
	global channel
	session = Session()
	if(body==None): return 
	#print("0")
	catch=json.loads(body)
	catch=json.loads(catch)
	local_has_message='A'
	local_does_not_have_message='B'
	local_message_left=0
	local_prev_has_message='A'
	if(flag==0):

		status = channel.queue_declare(queue=local_has_message)

		if status.method.message_count != 0:
			local_message_left=status.method.message_count
			local_has_message=local_has_message
			local_does_not_have_message=local_does_not_have_message
		#print("1 who has message ",has_message)
		#print("1 who doesnt have message",does_not_have_message)

		
		status = channel.queue_declare(queue=local_does_not_have_message)
		if status.method.message_count != 0:
			local_message_left=status.method.message_count
			local_prev_has_message=local_has_message
			local_has_message=local_does_not_have_message
			local_does_not_have_message=local_prev_has_message

		channel.basic_publish(exchange='', routing_key=local_has_message, body=body)
	
	
	if(catch["flag"]=="1"):
		#print("ppppppppp")
		result=session.query(user_table).filter_by(username=catch["username"]).all()
		if(result != None and len(result)==1):
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="0")
			ch.basic_ack(delivery_tag=method.delivery_tag)
			ch.stop_consuming()
			#connection.close()
			return 0

		entry=user_table(username=catch["username"],pwd=catch["password"])
		#create_all()

		session.add(entry)

		session.commit()
		#print("99")
		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="1")
		
		ch.basic_ack(delivery_tag=method.delivery_tag)
		ch.stop_consuming()		
		channel_sync.basic_publish(exchange='sync_exchange',routing_key='sync',body=body)
		return 1





	if(catch["flag"]=="2"):
		#print("write_api 2")
		user=catch["username"].strip("<")                                     
		user=user.strip(">")
		result=session.query(user_table).filter_by(username=catch["username"]).all()
		if(len(result)<1):
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="0")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()
			#connection.close()
			session.close()
			return 		
		
		del_entry=session.query(user_table).filter_by(username=catch["username"]).first()
		session.delete(del_entry)
		session.commit()
	
		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="2")
		ch.basic_ack(delivery_tag=method.delivery_tag)



		channel_sync.basic_publish(exchange='sync_exchange',routing_key='sync',body=body)

		ch.stop_consuming()
		#connection.close()
		session.close()			
		return

 
	if(catch["flag"]=="3"):
		creator=catch["created_by"]
		timestamp=catch["timestamp"]
		destination=int(catch["destination"].strip("{").strip("}"))	
		source=int(catch["source"].strip("{").strip("}"))
		
		if(int(source) < 1 or int(destination) > 198 or int(source) > 198 or int(destination) < 1):
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="0")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()
			session.close()
			#connection.close()
			return 
		
		data={"flag1":3}
		r = requests.get('http://RideShare-1572099024.us-east-1.elb.amazonaws.com:80/api/v1/users/', json=data)
		#return r.text
		#x=list(r.text)
		#return x[0]
		#a=r.json()["users"]
		#print("r.text",r.text)
		#print("type ",type(json.loads(r.text)))
		#print("r.json()",r.json())
		#print("JSON LOAD",json.loads(r.text))
		#print("json[]",json.loads(r.text)["users"])
		#print("len ",len(json.loads(r.text)["users"]))
		a=json.loads(r.text)["users"]
		#catch=json.loads(catch)
		
		var=0
		#print("creat",creator)		
		for x in a:		
			#print("10")
			#print("x",x)
			if(str(creator)==x):
				#print("11")
				var=1
		if(var==0):
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="1")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()
			session.close()
			#connection.close()
			return 

		
	
		b=timestamp.split(":")
		c=b[0].split("-")
		d=b[1].split("-")
		time=c[2]+"-"+c[1]+"-"+c[0]+" "+d[2]+":"+d[1]+":"+d[0]
		
		#t_s=datetime.datetime(int(c[2]),int(c[1]),int(c[0]),int(d[2]),int(d[1]),int(d[0]))
		t_s=str(timestamp).replace("-",":")
		#print("t",t_s)
		global ride_id_global
		
		result=session.query(ride_table).filter_by(source=int(source),destination=int(destination),creator=str(creator),create_time=t_s).all()
		res=[]
		for r in result:
			res.append(r.ride_id)
	
		#if(len(res)>0):
		#	return "3"
		#abort(400,'{"message":"RIDE already exists"}')
		
		new_ride_id=0
		result=session.query(ride_id_table).filter_by()
		for r in result:
			#db.create_all()		
			
			new_ride_id=int(r.ride_id)+int(1)
			session.delete(r)	
			session.commit()
			entry=ride_id_table(ride_id=int(new_ride_id))
			#db.create_all()		

			session.add(entry)
			session.commit()
		result=session.query(ride_id_table).filter_by()
		#for r in result:
		#	print("here ",r.ride_id)
		entry=ride_table(ride_id=new_ride_id,source=int(source),destination=int(destination),creator=str(creator),create_time=t_s)
		#db.create_all()
		session.add(entry)
		session.commit()
		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="2")
		ch.basic_ack(delivery_tag=method.delivery_tag)


		channel_sync.basic_publish(exchange='sync_exchange',routing_key='sync',body=body)
		ch.stop_consuming()
		session.close()
		#connection.close()	
		return 


	if(catch["flag"]=="6"):
		#print("write_api_6")
		username = catch["username"]
		ride=catch["ride_query"]
		ride=ride.strip("<")
		ride=ride.strip(">")
		result=session.query(ride_table).filter_by(ride_id=int(ride)).all()
		res=[]
		
		for r in result:
			res.append(r.ride_id)
		if(len(res)<1):
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="0")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()			
			#connection.close()
			session.close()
			return 
			#return "0"
			#abort(400,'{"message :Ride Not found"}')
		data={}
		data["flag1"]="6"
		data["username"]=username
		
		r = requests.get('http://RideShare-1572099024.us-east-1.elb.amazonaws.com/api/v1/users/', json=data)
		a=r.json()["users"]

		var=0
		#print("user",username)
		for x in a:		
			if(username==x):
				var=1
		if(var==0):
			#print("in")
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
	                        props.correlation_id),body="1")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()
			#connection.close()
			session.close()
			return 

			#return "1"
		
		result1=session.query(riders_table).filter_by(ride_id=int(ride),rider=str(username))
		res1=[]
		for r in result1:
			res1.append(r.ride_id)
		
		if(len(res1)>0):
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
	                        props.correlation_id),body="2")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()
			#connection.close()
			session.close()
			return 

			#return "2"		
		entry=riders_table(ride_id=int(ride),rider=str(username))
		#db.create_all()
		session.add(entry)
		session.commit()
		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="3")
		ch.basic_ack(delivery_tag=method.delivery_tag)

		channel_sync.basic_publish(exchange='sync_exchange',routing_key='sync',body=body)

		ch.stop_consuming()
		#connection.close()
		session.close()
		return 
		
		#return "3" #200 OK success"

	if(catch["flag"]=="7"):
				rideid=catch["id"]#.strip("<")
				#rideid=rideid.strip(">")
				result=session.query(ride_table).filter_by(ride_id=rideid)
				res=[]
				for  r in result:
					res.append(r.ride_id)
				rows_count=len(res)	
				if(rows_count<1):
					ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body="0")
					ch.basic_ack(delivery_tag=method.delivery_tag)
					#print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
					ch.stop_consuming()
					#connection.close()
					session.close()
					return 

					#return "0"
					#abort(400,'{"message":"Ride DOES NOT EXIST"}')	
				del_entry=session.query(ride_table).filter_by(ride_id=rideid)
				for r in del_entry:
					session.delete(r)
					session.commit()
				#print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
				del_entry1=session.query(riders_table).filter_by(ride_id=rideid)
				for r in del_entry1:
					session.delete(r)
					session.commit()
				#print("llllllllllllllllllllllllllllllllll")
				ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
				        props.correlation_id),body="1")
				ch.basic_ack(delivery_tag=method.delivery_tag)
				#print('oooooooooooooooooooooooooooooo')
				channel_sync.basic_publish(exchange='sync_exchange',routing_key='sync',body=body)

				ch.stop_consuming
				session.close()
				#connection.close()
				#print("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwww")
				return 

				
				#return "1"#"200 OK success"
	if(catch["flag"]=="11"):
		count=0
		del_entry=session.query(ride_id_table).filter_by().all()
		for r in del_entry:
			session.delete(r)	
			session.commit()

		del_entry=session.query(ride_table).filter_by().all()
		res=[]
		for x in del_entry:
			res.append(x.ride_id)
		if(len(res)!=0):
			count=count+1
			"""
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			connection.close()

			return 
			"""
			#return "0"
			
		for r in del_entry:
			session.delete(r)	
			session.commit()


		del_entry_1=session.query(riders_table).filter_by().all()
		res1=[]
		for x in del_entry_1:
			res1.append(x.ride_id)

		if(len(res1)!=0):
			count=count+1
			"""
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			connection.close()

			return 
			#return "0"
			"""		
		for r in del_entry_1:
			session.delete(r)	
			session.commit()
		
		del_entry=session.query(user_table).filter_by().all()
		deletions=[]
		for r in del_entry:
			deletions.append(r.username)
		if(len(deletions)!=0):
			count=count+1
			"""
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			connection.close()
			print("5")
			return 
			"""

		for r in del_entry:
			session.delete(r)
			session.commit()
		"""
		if(count!=3):		
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			ch.stop_consuming()
			session.close()
			#connection.close()
			return 

		"""

		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="1")
		ch.basic_ack(delivery_tag=method.delivery_tag)

		channel_sync.basic_publish(exchange='sync_exchange',routing_key='sync',body=body)
		ch.stop_consuming()
		session.close()
		#connection.close()
		return 


	session.close()


def read_function(ch, method, props, body):
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	lock.acquire()
	session = Session()
	#print("len ",len(str(body)))
	if((body==None) or (str(body)=="b\'\'")):
		#print("read stop")
		#ch.basic_ack(delivery_tag=method.delivery_tag)
		#ch.stop_consuming()
		session.close()
		lock.release()
		return 
	
	catch=json.loads(body)
	catch=json.loads(catch)
	

	if(catch["flag"]=="4"):
		#ch.basic_publish(exchange='sync_exchange',routing_key='sync',body="")
		
		#cur_time = datetime.datetime.strptime(str(catch["time"]),"%Y-%m-%d %H:%M:%S")
		cur_time=catch["time"]
		result=session.query(ride_table).filter_by(source=int(catch["source"]),destination=int(catch["destination"])).all()		
		#return "0"
		res=[]
		for r in result:
			db_time=str(r.create_time).split(":")
			for z in range(len(db_time)):
				db_time[z]=int(db_time[z])
			comp_time=datetime.datetime(db_time[2],db_time[1], db_time[0],db_time[5], db_time[4], db_time[3],0)
			cur_time=datetime.datetime.now()			
			if(comp_time>cur_time):
				res_dict={}
				res_dict["username"]=r.creator
				res_dict["rideId"]=r.ride_id
				res_dict["timestamp"]=r.create_time
				res.append(res_dict)
	
		if(len(res)==0):
			session.close()

			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()
			lock.release()
			session.close()
			return 0
			#0

		for i in res:
			a = str(i["timestamp"])
			a=str(a).replace(":","-")
			first_part=a[0:10]
			second_part=a[11:]
			tt=first_part+":"+second_part
			i["timestamp"] = tt
			i["rideId"] = int(i["rideId"])
		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body=json.dumps(res))
		ch.basic_ack(delivery_tag=method.delivery_tag)
		ch.stop_consuming()
		session.close()
		lock.release()
		#connection.close()
		return 1
	if(catch["flag"]=="5"):
		#ch.basic_publish(exchange='sync_exchange',routing_key='sync',body="")
		#print("readapi_2 2")
		ride=catch["ride"]
		r=session.query(ride_table).filter_by(ride_id=ride).all()
		temp=[]	
		
		for te in r:
			temp.append(r)
		if(len(temp)==0):
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="100")
			ch.basic_ack(delivery_tag=method.delivery_tag)
			
			ch.stop_consuming()

			session.close()
			lock.release()
			#connection.close()
			return 
			#return "0"		
		res=[]
		for x in r:
			#print(x.ride_id)
			temp={}
			temp["rideId"]=str(x.ride_id)
			a=str(x.create_time)
			a=str(a).replace(":","-")			
			first_part=a[0:10]
			second_part=a[11:]
			tt=first_part+":"+second_part
			
			temp["timestamp"]=tt
			temp["source"]=str(x.source)
			temp["destination"]=str(x.destination)
			temp["created_by"]=str(x.creator)
			res.append(temp)
		res1=res[0]
		result2=session.query(riders_table).filter_by(ride_id=ride).all()
		res2=[]
		for r in result2:
			res2.append(r.rider)
		res1["users"]=res2
		#print("res1", res1)
		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        props.correlation_id),body=json.dumps(res1))

		ch.basic_ack(delivery_tag=method.delivery_tag)
		ch.stop_consuming()
		session.close()
		#connection.close()
		lock.release()
		return 
		#return jsonify(res)
	if(catch["flag"]=="10"):
			#ch.basic_publish(exchange='sync_exchange',routing_key='sync',body="")
			#print("api 1000000000000000000000000000")
			if("flag1" in catch):
				#return "here"
				result=session.query(user_table).filter_by().all()
				res={"users":[]}
						
				for r in result:
					res["users"].append(r.username)
				#print("res in read",res)
				#print("json dumps",json.dumps(res))
				#print("res[users]",res["users"])


					
				if(len(res["users"])==0):
					ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
						props.correlation_id),body="0")

					ch.basic_ack(delivery_tag=method.delivery_tag)
					#connection.close()
					session.close()
					lock.release()
					return 

					return "0"
				ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
				        props.correlation_id),body=json.dumps(res))

				ch.basic_ack(delivery_tag=method.delivery_tag)
				#connection.close()
				session.close()
				ch.stop_consuming()	
				lock.release()
				return 

				#return jsonify(res)
				
			result=session.query(user_table).filter_by().all()
			res={"users":[]}
							
			for r in result:
				res["users"].append(r.username)	
			if(len(res["users"])==0):
				ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
				        props.correlation_id),body="0")

				ch.basic_ack(delivery_tag=method.delivery_tag)
				#connection.close()
				session.close()
				lock.release()
				ch.stop_consuming()
				return 


			body={}
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body=json.dumps(res["users"]))

			ch.basic_ack(delivery_tag=method.delivery_tag)

			#connection.close()
			session.close()
			ch.stop_consuming()
			lock.release()
			return 


		
	if(catch["flag"]=="14"):
		#ch.basic_publish(exchange='sync_exchange',routing_key='sync',body="")

		rows = session.query(ride_table).filter_by().count()
		ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body=str(rows))
		ch.basic_ack(delivery_tag=method.delivery_tag)

		#connection.close()
		session.close()
		ch.stop_consuming()
		lock.release()
		return 


	#session.close()
def sync_function(ch, method, props, body):
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	lock.acquire()
	session = Session()
	


	if((body==None) or (str(body))=="b\'\'"):
		lock.release()
		return 1
	catch=json.loads((body))
	catch=json.loads(catch)
	#catch=json.loads(catch)
	if(catch["flag"]=="1"):

		result=session.query(user_table).filter_by(username=catch["username"]).all()
		if(result != None and len(result)==1):
			ch.basic_ack(delivery_tag=method.delivery_tag)
			session.close()
			lock.release()

			return 0

		entry=user_table(username=catch["username"],pwd=catch["password"])
		#create_all()

		session.add(entry)

		session.commit()
		#ch.basic_ack(delivery_tag=method.delivery_tag)
		ch.basic_ack(delivery_tag=method.delivery_tag)
		ch.stop_consuming()
		#connection.close()

		session.close()
		lock.release()

		return 1


	if(catch["flag"]=="2"):
		#ch.basic_publish(exchange='orchestrator_exchange',routing_key='read',body="")

		user=catch["username"].strip("<")                                     
		user=user.strip(">")
		result=session.query(user_table).filter_by(username=catch["username"]).all()
		if(len(result)<1):
			#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        #props.correlation_id),body="0")
			#ch.basic_ack(delivery_tag=method.delivery_tag)
			ch.stop_consuming()
			ch.basic_ack(delivery_tag=method.delivery_tag)
			#connection.close()
			session.close()
			lock.release()
			return 		
		
		del_entry=session.query(user_table).filter_by(username=catch["username"]).first()
		session.delete(del_entry)
		session.commit()
	
		#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                #        props.correlation_id),body="2")
		#ch.basic_ack(delivery_tag=method.delivery_tag)
		#connection.close()
		ch.basic_ack(delivery_tag=method.delivery_tag)		
		ch.stop_consuming()			

		lock.release()	
		session.close()	
		return 
	if(catch["flag"]=="3"):
		#ch.basic_publish(exchange='orchestrator_exchange',routing_key='read',body="")
		creator=catch["created_by"]
		timestamp=catch["timestamp"]
		destination=int(catch["destination"].strip("{").strip("}"))	
		source=int(catch["source"].strip("{").strip("}"))
		
		if(int(source) < 1 or int(destination) > 198 or int(source) > 198 or int(destination) < 1):
			#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        #props.correlation_id),body="0")
			ch.basic_ack(delivery_tag=method.delivery_tag)
			ch.stop_consuming()
			session.close()
			lock.release()
			return 
		b=timestamp.split(":")
		c=b[0].split("-")
		d=b[1].split("-")
		time=c[2]+"-"+c[1]+"-"+c[0]+" "+d[2]+":"+d[1]+":"+d[0]
		
		#t_s=datetime.datetime(int(c[2]),int(c[1]),int(c[0]),int(d[2]),int(d[1]),int(d[0]))
		t_s=str(timestamp).replace("-",":")
		#print("t",t_s)
		global ride_id_global
		
		result=session.query(ride_table).filter_by(source=int(source),destination=int(destination),creator=str(creator),create_time=t_s).all()
		res=[]
		for r in result:
			res.append(r.ride_id)
	
		
		new_ride_id=0
		result=session.query(ride_id_table).filter_by()
		for r in result:
			#db.create_all()		
			
			new_ride_id=int(r.ride_id)+int(1)
			session.delete(r)	
			session.commit()
			entry=ride_id_table(ride_id=int(new_ride_id))
			#db.create_all()		

			session.add(entry)
			session.commit()
		result=session.query(ride_id_table).filter_by()
		entry=ride_table(ride_id=new_ride_id,source=int(source),destination=int(destination),creator=str(creator),create_time=t_s)
		#db.create_all()
		session.add(entry)
		session.commit()
		#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                #       props.correlation_id),body="2")
		ch.basic_ack(delivery_tag=method.delivery_tag)
		ch.stop_consuming()

		#connection.close()
		session.close()
		lock.release()
		return 


	if(catch["flag"]=="6"):
		#ch.basic_ack(delivery_tag=method.delivery_tag)
		#ch.basic_publish(exchange='orchestrator_exchange',routing_key='read',body="")
		username = catch["username"]
		ride=catch["ride_query"]
		ride=ride.strip("<")
		ride=ride.strip(">")
		result=session.query(ride_table).filter_by(ride_id=int(ride)).all()
		res=[]
		
		for r in result:
			res.append(r.ride_id)
		if(len(res)>1):

			
			#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        #props.correlation_id),body="0")
			ch.basic_ack(delivery_tag=method.delivery_tag)

			ch.stop_consuming()
			#connection.close()
			session.close()
			lock.release()
			return 
			#return "0"
			#abort(400,'{"message :Ride Not found"}')
		
		result1=session.query(riders_table).filter_by(ride_id=int(ride),rider=str(username))
		res1=[]
		for r in result1:
			res1.append(r.ride_id)
		
		if(len(res1)>0):
			#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
	                #        props.correlation_id),body="2")
			ch.basic_ack(delivery_tag=method.delivery_tag)
			ch.stop_consuming()
			#connection.close()
			lock.release()
			session.close()
			return 

			#return "2"		
		entry=riders_table(ride_id=int(ride),rider=str(username))
		#db.create_all()
		session.add(entry)
		session.commit()
		#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                #        props.correlation_id),body="3")
		#ch.basic_ack(delivery_tag=method.delivery_tag)
		#print("sync api 6 done")
		ch.basic_ack(delivery_tag=method.delivery_tag)
		ch.stop_consuming()
		session.close()
		lock.release()
		return 
		
		#return "3" #200 OK success"
	if(catch["flag"]=="7"):
				#ch.basic_publish(exchange='orchestrator_exchange',routing_key='read',body="")

				rideid=catch["id"]#.strip("<")
				#rideid=rideid.strip(">")
				result=session.query(ride_table).filter_by(ride_id=rideid)
				res=[]
				for  r in result:
					res.append(r.ride_id)
				rows_count=len(res)	
				if(rows_count<1):
					#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
                        #props.correlation_id),body="0")
					ch.basic_ack(delivery_tag=method.delivery_tag)
					ch.stop_consuming()
					session.close()
					lock.release()
					return 

					#return "0"
					#abort(400,'{"message":"Ride DOES NOT EXIST"}')	
				del_entry=session.query(ride_table).filter_by(ride_id=rideid)
				for r in del_entry:
					session.delete(r)
					session.commit()

				del_entry1=session.query(riders_table).filter_by(ride_id=rideid)
				for r in del_entry1:
					session.delete(r)
					session.commit()
					#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
				        #props.correlation_id),body="1")
				ch.basic_ack(delivery_tag=method.delivery_tag)
				ch.stop_consuming()
				session.close()
				lock.release()
				return 

				
				#return "1"#"200 OK success"
	if(catch["flag"]=="11"):
		#ch.basic_publish(exchange='orchestrator_exchange',routing_key='read',body="")
		#ch.basic_ack(delivery_tag=method.delivery_tag)
		count=0
		del_entry=session.query(ride_id_table).filter_by().all()
		for r in del_entry:
			session.delete(r)	
			session.commit()

		del_entry=session.query(ride_table).filter_by().all()
		res=[]
		for x in del_entry:
			res.append(x.ride_id)
		if(len(res)!=0):
			count=count+1
			"""
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			connection.close()

			return 
			"""
			#return "0"
			
		for r in del_entry:
			session.delete(r)	
			session.commit()


		del_entry_1=session.query(riders_table).filter_by().all()
		res1=[]
		for x in del_entry_1:
			res1.append(x.ride_id)

		if(len(res1)!=0):
			count=count+1
			"""
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			connection.close()

			return 
			#return "0"
			"""		
		for r in del_entry_1:
			session.delete(r)	
			session.commit()
		

		del_entry=session.query(user_table).filter_by().all()
		deletions=[]
		for r in del_entry:
			deletions.append(r.username)
		if(len(deletions)!=0):
			count=count+1
			"""
			ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		                props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			connection.close()
			print("5")
			return 
			"""

		for r in del_entry:
			session.delete(r)
			session.commit()

		if(count!=3):		
			#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		         #       props.correlation_id),body="0")

			ch.basic_ack(delivery_tag=method.delivery_tag)
			#connection.close()
			ch.stop_consuming()			
			session.close()
			lock.release()
			return 



		#ch.basic_publish(exchange='',routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = \
		 #               props.correlation_id),body="1")
		#ch.basic_ack(delivery_tag=method.delivery_tag)
		#connection.close()
		ch.basic_ack(delivery_tag=method.delivery_tag)
		#stop_consuming()
		session.close()
		lock.release()
		return 


	session.close()
def convert_db_to_json(channel,method_frame,header_frame,body):
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	session = Session()
	whole_db=[]

	user_t=session.query(user_table).filter_by().all()
	rowarray_list_a = []
	for row in user_t:
	    t = (row.username, row.pwd)
	    rowarray_list_a.append(t)

	whole_db.append(rowarray_list_a)

	ride_t=session.query(ride_table).filter_by().all()
	rowarray_list_b = []
	for row in ride_t:
	    t = (row.ride_id, row.source, row.destination, row.creator, row.create_time)
	    rowarray_list_b.append(t)
	
	whole_db.append(rowarray_list_b)

	riders_t=session.query(riders_table).filter_by().all()
	rowarray_list_c = []
	for row in riders_t:
	    t = (row.ride_id, row.rider)
	    rowarray_list_c.append(t)
	
	whole_db.append(rowarray_list_c)


	ride_id_t=session.query(ride_id_table).filter_by().all()
	rowarray_list_d = []
	for row in ride_id_t:
	    t = (row.ride_id)
	    rowarray_list_d.append(t)
	
	whole_db.append(rowarray_list_d)

	#print("WHOLE DB",whole_db)
	body = json.dumps(whole_db)
	channel.basic_publish(exchange='slave_sync_exchange',routing_key='response',body=body)
	session.close()
	return 
	
def convert_json_to_db(channel,method_frame,header_frame,body):  
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock      
	session = Session()
	whole_db=json.loads(body)
	#print("whole DB",whole_db)
	user_t=whole_db[0]

	for row in user_t:
		
		entry=user_table(username=row[0],pwd=row[1])	
		session.add(entry)
		session.commit()
	ride_t=whole_db[1]

	for row in ride_t:
		entry=ride_table(ride_id=row[0],source=row[1],destination=row[2],creator=row[3],create_time=row[4])
		session.add(entry)
		session.commit()
	riders_t=whole_db[2]

	for row in riders_t:
		entry=riders_table(ride_id=row[0],rider=row[1])
		session.add(entry)
		session.commit()

	ride_id_t=whole_db[3]

	for row in ride_id_t:
		if(row!=0):
			entry=ride_id_table(ride_id=row)
			session.add(entry)
			session.commit()

	session.close()
	return


def master():
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	global channel
	#print("waiting for write req")
	method_frame, header_frame, body = channel.basic_get(queue = 'write_queue')        
	if method_frame!=None and method_frame.NAME == 'Basic.GetEmpty':
		#connection.close()
		#print("no write found")
		return 
	else:    
		write_function(channel,method_frame,header_frame,body,0)        
		#print("write_end")
		return 
		#channel.basic_ack(delivery_tag=method_frame.delivery_tag)
		#connection.close() 
		

	#print("end of master fun")



def slave():
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	t1 = Thread(target=sync_thread, args=())
	t2 = Thread(target=read_thread, args=())
	t1.start()
	t2.start()
	t1.join()
	#print("t1 joined")
	t2.join()
	#print("t2 joined")
	return

def read_thread():

	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	global channel
	#print("waiting for read req")
	#channel.start_consuming()
	method_frame, header_frame, body = channel.basic_get(queue = 'read_queue')        
	if method_frame!=None and method_frame.NAME == 'Basic.GetEmpty':
		#connection.close()
		#print("no read req found")
		return 
	else:    
		read_function(channel,method_frame,header_frame,body)        
		#print("read end")
		return 
		#channel.basic_ack(delivery_tag=method_frame.delivery_tag)
		#connection.close() 
		#return body
	



	#print("end of read_thread")
	#connection.close()	

def sync_thread():
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	#print("waiting for sync req")
	method_frame, header_frame, body = channel_sync.basic_get(queue = sync_queue.method.queue,auto_ack=False)        
	if method_frame!=None and method_frame.NAME == 'Basic.GetEmpty':
		#connection.close()
		#print("no sync found")
		return 
	else:    
		sync_function(channel_sync,method_frame,header_frame,body)        
		#print("sync_end")
		return 
		#channel.basic_ack(delivery_tag=method_frame.delivery_tag)
		#connection.close() 
		

	#print("end of sync_thread")
	#connection.close()
def check_new_sync_request():
		global sync_queue
		global channel_sync
		global Base
		global engine
		global Session
		global lock
		global channel
		
		method_frame, header_frame, body = channel.basic_get(queue = 'request_sync_queue',auto_ack=False)  
		#print(method_frame, header_frame, body)
		#print("MMMMMMMMMMMMM",method_frame!=None)#,method_frame.NAME)      
		if(method_frame!=None and method_frame.NAME == 'Basic.GetEmpty'):
			x=1
		elif(method_frame!=None):    
				convert_db_to_json(channel,method_frame,header_frame,body)        

		return 

def exit_handler(channel,queue_name):
	channel.queue_delete(queue=queue_name)

#atexit.register(exit_handler,channel_sync,sync_queue.method.queue)
def worker():
	global sync_queue
	global channel_sync
	global Base
	global engine
	global Session
	global lock
	global channel
	global firsttime
	global switch2master
	var =True
	while(True):
			if(firsttime or switch2master):
				switch2master=True
				firsttime=False
				#print("MASTER")
				check_new_sync_request()
				master()
				#connection.close()networnetwor
				#print("con close mas")
			else :
				if(var):
					corr_id = str(uuid.uuid4())
					channel.basic_publish(exchange='slave_sync_exchange',routing_key='request',\
	properties=pika.BasicProperties(correlation_id=corr_id),body="999")

					flag=True
					while(flag):
						method_frame, header_frame, body = channel.basic_get(queue\
	='response_sync_queue',auto_ack=False)        
						if method_frame!=None and method_frame.NAME == 'Basic.GetEmpty':
								x=1		
						elif(method_frame!=None):    
								convert_json_to_db(channel,method_frame,header_frame,body)        
								flag=False
								channel.basic_ack(delivery_tag=method_frame.delivery_tag)
					var =False

				#print("SLAVE")
				slave()


def my_listener(state):
	if(state == KazooState.LOST):
		xasdad=0
	elif(state == KazooState.SUSPENDED):
		xdasdsad=0
	else:
		dfafafa=0

def find_data(mynode):
	global zk
	mynodedata,stat=zk.get("/election/%s"%mynode)
	return mynodedata
	
def find_lowest(children):
	global zk
	children3=[]
	for t in children:
		data = find_data(t)
		if(t.find("master")==-1):
			children3.append((t,int(data)))
	minEle=99999999
	mindId=99999999
	for t in children3:
		if(t[1]<minEle):
			minEle=t[1]
			minId=t[0]
	return mindId, minEle

def find_prev_max(mynode, children):
	global zk
	global switch2master
	mynodedata = find_data(mynode)
	children3=[]
	for t in children:
		data = find_data(t)
		if(t.find("master")==-1):
			children3.append((t,int(data)))
			
	minid, minEle = find_lowest(children)
	if int(mynodedata)==minEle:
		prev_node = mynode
		if not zk.exists("/election/master"): 
			lowest_znode_id, lowest_znode_data = find_lowest(children)
			mynodedata=find_data(mynode)
			if int(mynodedata)==int(lowest_znode_data):
				mynodedata=str(mynodedata)
				switch2master=True
				if zk.exists("/slave_ft/%s"%slave_znode):
					zk.delete("/slave_ft/%s"%slave_znode)
				if not zk.exists("/election/master"):
					zk.create("/election/master", mynodedata.encode('utf8'), ephemeral= True)
	else:

		children4=[]
		for t in children3:
			if t[1]<int(mynodedata):
				children4.append(t)
		maxdata=int(children4[0][1])
		maxid=children4[0][0]
		for t in children4:
			if(t[1]>=int(maxdata)):
				maxdata=int(t[1])
				maxid=t[0]
		prev_node = maxid
	return prev_node
	

def elect_leader_now(event):
	global mynode
	children = zk.get_children("/election")
	
	elect_leader(mynode, children)

def elect_leader(mynode, children):
	global slave_znode
	global zk
	global switch2master
	prev_node = find_prev_max(mynode, children)
	if not zk.exists("/election/master"):
		lowest_znode_id, lowest_znode_data = find_lowest(children)
		mynodedata=find_data(mynode)
		if int(mynodedata)==int(lowest_znode_data):
			mynodedata = str(mynodedata)
			switch2master=True
			if zk.exists("/slave_ft/%s"%slave_znode):
				zk.delete("/slave_ft/%s"%slave_znode)
			if not zk.exists("/election/master"):
				zk.create("/election/master", mynodedata.encode('utf8'), ephemeral= True)

	#print("Watch from %s on %s"%(mynode, prev_node))
	zk.exists("/election/%s"%prev_node, watch=elect_leader_now)

def zookeeper():
	global zk
	flag=0
	global mynode
	while True:
		if flag==0:
			flag=1
			str2 = str(os.getpid())
			time.sleep(15)
			mynode=zk.create("/election/", str2.encode('utf8'), sequence= True, ephemeral= True)
			mynode = mynode[10:]
			children = zk.get_children("/election")
			elect_leader(mynode, children)
		time.sleep(3)
		children = zk.get_children("/election")
		find_prev_max(mynode, children)

if __name__ == "__main__":
	switch2master=False
	mynode=0
	children_w=0
	session = Session()
	res=session.query(ride_id_table).filter_by().count()
	if(res==0):
		
		entry=ride_id_table(ride_id=int(0))
		session.add(entry)
		session.commit()

	session.close()
	credential_params = pika.PlainCredentials('guest', 'guest')
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials = credential_params,heartbeat=0))
	channel = connection.channel()
	channel.basic_qos(prefetch_count=1)
	channel.exchange_declare(exchange='orchestrator_exchange', exchange_type='direct')
	channel.queue_declare(queue='write_queue', durable=True)
	channel.queue_declare(queue='read_queue', durable=True)
	channel.queue_declare(queue='write_response_queue', durable=True)
	channel.queue_declare(queue='read_response_queue', durable=True)

	#print("done1")
	channel.queue_bind(exchange='orchestrator_exchange', queue="read_queue",routing_key="read")
	channel.queue_bind(exchange='orchestrator_exchange', queue='write_queue',routing_key="write")

	channel.exchange_declare(exchange='slave_sync_exchange', exchange_type='direct')
	channel.queue_declare(queue='request_sync_queue', durable=True)
	channel.queue_declare(queue='response_sync_queue', durable=True)
	channel.queue_bind(exchange='slave_sync_exchange', queue="request_sync_queue",routing_key="request")
	channel.queue_bind(exchange='slave_sync_exchange', queue='response_sync_queue',routing_key="response")
	#print("done2")
	credential_params = pika.PlainCredentials('guest', 'guest')
	connection_sync = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials = credential_params,heartbeat=0))
	channel_sync = connection_sync.channel()
	channel_sync.basic_qos(prefetch_count=1)
	channel_sync.exchange_declare(exchange='sync_exchange', exchange_type='fanout')
	#channel_sync.queue_declare(queue='sync_queue', durable=True)
	#global sync_queue
	#print("done3")
	
	if(sync_queue==None):
		#print("hereeererer")
		sync_queue = channel_sync.queue_declare(queue='')
		atexit.register(exit_handler,channel_sync,sync_queue.method.queue)
		#print("sync q name",sync_queue.method.queue)
	channel_sync.queue_bind(exchange='sync_exchange', queue=sync_queue.method.queue	,routing_key="sync")
	
	firsttime=False
	
	#docker daemon conn
	client = docker.from_env()
	cli = docker.APIClient(base_url='unix:///var/run/docker.sock')
	#print("done4")
	
	conts = client.containers.list(all=True)
	#print("done10")
	zk = KazooClient(hosts='zoo:2181')
	zk.add_listener(my_listener)
	zk.start()
	slave_znode=0
	name=""
	for cont in conts:
		name=cont.name
		contpid = cli.inspect_container(name)['State']['Pid']
		#print("done11")
		if(name=="master" and contpid==os.getpid()):
			firsttime=True
			#print("done12")
			break

	for cont in conts:
		name=cont.name
		contpid = cli.inspect_container(name)['State']['Pid']
		#print("done11")
		if(contpid==os.getpid()):
			if(name.find("worker")!=-1):
				str2=str(os.getpid())
				slave_znode=zk.create("/slave_ft/", str2.encode('utf8'), sequence= True, ephemeral= True)[10:]
				#print("SLAVE ZNODE : %s"%slave_znode)
				break
			

	#print("done5")

	t1 = Thread(target=worker, args=())
	t2 = Thread(target=zookeeper, args=())
	t1.start()
	t2.start()
	t1.join()
	#print("t1 joined")
	t2.join()

