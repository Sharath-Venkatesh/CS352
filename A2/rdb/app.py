from flask import Flask, render_template,\
jsonify, request, abort,Response
import requests
import json
import datetime
import re
import datetime 

from flask_sqlalchemy import SQLAlchemy

app=Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"]='sqlite:///rides.db'
db=SQLAlchemy(app)





ride_id_global=0

class ride_table(db.Model):
	__tablename__='ride_table'
	
	ride_id=db.Column('ride_id',db.Integer,primary_key=True)
	source=db.Column('source',db.Integer)
	destination=db.Column('destination',db.Integer)
	creator=db.Column('creator',db.String(40))
	create_time=db.Column('create_time',db.DateTime)
	
	def ride_table(self):
		return f"ride_table('{self.ride_id}','{self.source}','{self.destination}','{self.creator}','{self.create_time}')"

class riders_table(db.Model):
	__tablename__='riders_table'
	ride_id=db.Column('ride_id',db.Integer,primary_key=True)
	rider=db.Column('rider',db.String(40),primary_key=True)
	def riders_table(self):
		return f"riders_table('{self.ride_id}','{self.rider}')"



# API 3 create new ride

@app.route('/api/v1/rides',methods=["POST"])
def create_new_ride():
	ride = request.get_json()
	ride["flag"]="3"
	r = requests.post('http://127.0.0.1:80/api/v1/db/write', json=ride)
	#return r.text
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='Not valid source or destination')),status=400, mimetype='application/json')
		return response
	

	if(r.text=="1"):
		response = Response(response=json.dumps(dict(error='User does not exist')),status=400, mimetype='application/json')
		return response
	if(r.text=="2"):
		response = Response(response=json.dumps(dict({})),status=201, mimetype='application/json')
		return response
	
	if(r.text=="3"):
		response = Response(response=json.dumps(dict(error='Ride already exists')),status=400, mimetype='application/json')
		return response

#API 4  GET

@app.route('/api/v1/rides', methods = ["GET"])
def rides():
	
	args = request.args
	args = dict(args)
	query = {}
	cur_time=str(datetime.datetime.now())
	a=cur_time.split(" ")
	b=a[1].split(":")
	c=b[2].split(".")
	act_time=a[0]+" "+b[0]+":"+b[1]+":"+c[0]
	act_time=str(act_time)
	query["flag"]="4"
	query["source"]=args["source"]
	query["destination"]=args["destination"]
	query["time"]=act_time	
	r = requests.post('http://127.0.0.1:80/api/v1/db/read',json=query)
	#return "hjj"	
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='')),status=400, mimetype='application/json')
		return response
	return r.text




#API 5 GET

@app.route('/api/v1/rides/<ride_id>', methods = ["GET"])
def ride_details(ride_id):
	query = {}
	query["flag"] = "5"
	
	query["ride"]= ride_id.strip("<").strip(">")
	r=requests.post("http://127.0.0.1:80/api/v1/db/read",json=query)
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='Ride doesnt exist')),status=400, mimetype='application/json')
		return response
	return r.text

# API 6 join a ride
@app.route('/api/v1/rides/<ride_query_ID>',methods=["POST"])
def join_ride(ride_query_ID):
	data=request.get_json()
	data["flag"]="6"
	data["ride_query"]=ride_query_ID.strip("<").strip(">")
	r = requests.post('http://127.0.0.1:80/api/v1/db/write', json=data)
	#return r.text
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='Ride Not found')),status=400, mimetype='application/json')
		return response
	if(r.text=="1"):
		response = Response(response=json.dumps(dict(error='User not found')),status=400, mimetype='application/json')
		return response
	if(r.text=="2"):
		response = Response(response=json.dumps(dict(error='User already in given ride')),status=400, mimetype='application/json')
		return response

	response = Response(response=json.dumps(dict({})),status=201, mimetype='application/json')
	return response

#API 7 delete
@app.route('/api/v1/rides/<ride_ID>', methods = ["Delete"])
def delete_ride(ride_ID):
	data={}
	data["flag"]="7"
	data["id"]=ride_ID.strip(">").strip("<")
	r = requests.delete('http://127.0.0.1:80/api/v1/db/write', json=data)
	
	#return r.text
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='Ride not found')),status=400, mimetype='application/json')
		return response
	response = Response(response=json.dumps(dict({})),status=200, mimetype='application/json')
	return response	


# API 8 write db
@app.route('/api/v1/db/write',methods=["PUT","DELETE","POST"])
def write_db():
	catch=request.get_json()
	if(catch["flag"]=="3"):
		creator=catch["created_by"]
		timestamp=catch["timestamp"]
		destination=int(catch["destination"].strip("{").strip("}"))	
		source=int(catch["source"].strip("{").strip("}"))
		
		if(int(source) < 1 or int(destination) > 198 or int(source) > 198 or int(destination) < 1):
			return "0"
		data={}
		r = requests.get('http://54.210.210.247:8080/api/v1/users/', json=data)

		a=r.json()["users"]
		var=0
		for x in a:		
			if(creator==x):
				var=1
		if(var==0):
			return "0"
	
		b=timestamp.split(":")
		c=b[0].split("-")
		d=b[1].split("-")
		time=c[2]+"-"+c[1]+"-"+c[0]+" "+d[2]+":"+d[1]+":"+d[0]
		
		t_s=datetime.datetime(int(c[2]),int(c[1]),int(c[0]),int(d[2]),int(d[1]),int(d[0]))
		
		global ride_id_global
		
		result=ride_table.query.filter_by(source=int(source),destination=int(destination),creator=str(creator),create_time=t_s).all()
		res=[]
		for r in result:
			res.append(r.ride_id)
	
		if(len(res)>0):
			return "3"
		#abort(400,'{"message":"RIDE already exists"}')
		
		ride_id_global=ride_id_global+1
		entry=ride_table(ride_id=ride_id_global,source=int(source),destination=int(destination),creator=str(creator),create_time=t_s)
		db.create_all()
		db.session.add(entry)
		db.session.commit()
		return "2" 

	if(catch["flag"]=="6"):
		username = catch["username"]
		ride=catch["ride_query"]
		ride=ride.strip("<")
		ride=ride.strip(">")
		result=ride_table.query.filter_by(ride_id=int(ride)).all()
		res=[]
		for r in result:
			res.append(r.ride_id)
		if(len(res)<1):
			return "0"
			#abort(400,'{"message :Ride Not found"}')
		
		
		data={}
		data["flag"]="6"
		data["username"]=username
		
		r = requests.get('http://54.210.210.247:8080/api/v1/users/', json=data)
		a=r.json()["users"]
		var=0
		for x in a:		
			if(username==x):
				var=1
		if(var==0):
			return "1"
			
		result1=riders_table.query.filter_by(ride_id=int(ride),rider=str(username))
		res1=[]
		for r in result1:
			res1.append(r.ride_id)
		
		if(len(res1)>0):
			return "2"		
		entry=riders_table(ride_id=int(ride),rider=str(username))
		db.create_all()
		db.session.add(entry)
		db.session.commit()
		
		return "3" #200 OK success"
	if(catch["flag"]=="7"):
						
				rideid=catch["id"].strip("<")
				rideid=rideid.strip(">")
				result=ride_table.query.filter_by(ride_id=rideid)
				res=[]
				for  r in result:
					res.append(r.ride_id)
				rows_count=len(res)	
				if(rows_count<1):
					return "0"
					#abort(400,'{"message":"Ride DOES NOT EXIST"}')	
				del_entry=ride_table.query.filter_by(ride_id=rideid)
				for r in del_entry:
					db.session.delete(r)
					db.session.commit()
				
				del_entry1=riders_table.query.filter_by(ride_id=rideid)
				for r in del_entry1:
					db.session.delete(r)
					db.session.commit()
				
				
				return "1"#"200 OK success"
	


#API 9  Read from DB
@app.route('/api/v1/db/read', methods = ["POST"])
def db_read():
	
	query = request.get_json()
	if(query["flag"]=="4"):

		cur_time = datetime.datetime.strptime(str(query["time"]),"%Y-%m-%d %H:%M:%S")
		
		result=ride_table.query.filter_by(source=int(query["source"]),destination=int(query["destination"])).all()		
		#return "0"
		res=[]
		for r in result:
			if(r.create_time>cur_time):
				res_dict={}
				res_dict["created_by"]=r.creator
				res_dict["rideId"]=r.ride_id
				res_dict["timestamp"]=r.create_time
				res.append(res_dict)
	
		if(len(res)==0):
			return "0"
		#return jsonify(hell)
		for i in res:
			a = str(i["timestamp"])
			date, time = a.split(' ')
			date = date.split('-')[::-1]
			date = date[0] + '-' + date[1] + '-' + date[2]
			time = time.split(':')[::-1]
			time = time[0] + '-' + time[1] + '-' + time[2]
			timestamp = date + ':' + time
			i["timestamp"] = timestamp
			i["rideId"] = int(i["rideId"])
		
		return jsonify(res)

	if(query["flag"]=="5"):
		ride=query["ride"]
		r=ride_table.query.filter_by(ride_id=ride).all()
		temp=[]	
		for te in r:
			temp.append(r)
		if(len(temp)==0):
			return "0"		
		res=[]
		for x in r:
			#print(x.ride_id)
			temp={}
			temp["ride_Id"]=x.ride_id
			temp["timestamp"]=str(x.create_time).replace(' ',':',1)
			temp["source"]=x.source
			temp["destination"]=x.destination
			temp["created_by"]=x.creator
			res.append(temp)
		res1=res[0]
		result2=riders_table.query.filter_by(ride_id=ride).all()
		res2=[]
		for r in result2:
			res2.append(r.rider)
		res1["users"]=res2
		return jsonify(res1)

#API 11 delete all ride
@app.route('/api/v1/db/clear', methods = ["POST"])
def clear_db():
	del_entry=ride_table.query.filter_by().all()
	res=[]
	for x in del_entry:
		res.append(x.ride_id)	
	for r in del_entry:
		db.session.delete(r)	
		db.session.commit()


	del_entry_1=riders_table.query.filter_by().all()
	res1=[]
	for x in del_entry_1:
		res1.append(x.ride_id)

	
		
	for r in del_entry_1:
		db.session.delete(r)	
		db.session.commit()
	

	response = Response(response=json.dumps(dict()),status=200, mimetype='application/json')
	return response
		

if __name__ == "__main__":
	db.create_all()
	app.run(debug = True, host = '0.0.0.0', port = 80)
		

