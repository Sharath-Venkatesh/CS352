from flask import Flask, render_template,\
jsonify, request, abort,Response,make_response
from json import  dumps

import requests
import json
import datetime
import re
import datetime 

from flask_sqlalchemy import SQLAlchemy


app=Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"]='sqlite:///user.db'
db=SQLAlchemy(app)


class user_table(db.Model):
	__tablename__='user_table'
	username=db.Column('username',db.String(40),primary_key=True)
	pwd=db.Column('pwd',db.String(40),primary_key=True)
	
	def __repr__(self):
		return f"user_table('{self.username}')"
def is_sha1(maybe_sha):
    if len(maybe_sha) != 40:
        return False
    try:
        sha_int = int(maybe_sha, 16)
    except ValueError:
        return False
    return True



# API 1 add user
@app.route('/api/v1/users', methods = ["PUT"])
def add_user():
	
	user = request.get_json()

	username = user["username"]

	pwd = user["password"]
	data={}	
	if(is_sha1(pwd)==False):
		response = Response(response=json.dumps(dict(error='Password not in format')),status=400, mimetype='application/json')
		return response
	data["flag"]="1"
	data["username"]=username
	data["pwd"]=pwd
	data["columns"]=["username","pwd"]
	data["values"]=[username,pwd]
	r = requests.put('http://127.0.0.1:80/api/v1/db/write', json=data)
	#return r.text
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='User already exists')),status=400, mimetype='application/json')
		return response

	response = Response(response=json.dumps(dict({})),status=201, mimetype='application/json')
	return response


# API 2 delete user
@app.route('/api/v1/users/<username>', methods = ["DELETE"])
def delete_user(username):
	data={}
	#return username
	data["flag"]="2"
	data["username"]=username
	r = requests.delete('http://127.0.0.1:80/api/v1/db/write', json=data)
	#return r.text
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='User doesnot exist')),status=400, mimetype='application/json')
		return response

	if(r.text=="1"):
		response = Response(response=json.dumps(dict(error='User cannot be deleted,he is associated with rides')),status=404, mimetype='application/json')
		return response

	response = Response(response=json.dumps(dict({})),status=200, mimetype='application/json')
	return response

# API 8 write db
@app.route('/api/v1/db/write',methods=["PUT","DELETE","POST"])
def write_db():
	catch=request.get_json()
	#return catch["flag"]#"something"
	if(catch["flag"]=="1"):
		
		result=user_table.query.filter_by(username=catch["username"]).all()
		if(result != None and len(result)==1):
			return "0"		
		entry=user_table(username=catch["username"],pwd=catch["pwd"])
		db.create_all()
		db.session.add(entry)
		db.session.commit()
		
		return "1"

	if(catch["flag"]=="2"):
		user=catch["username"].strip("<")                                     
		user=user.strip(">")
		result=user_table.query.filter_by(username=catch["username"]).all()
		if(len(result)<1):
			return "0"		
		
		del_entry=user_table.query.filter_by(username=catch["username"]).first()
		db.session.delete(del_entry)
					
		db.session.commit()
		
		return "2"

#API 10 list all users
@app.route('/api/v1/users/', methods = ["GET"])
def list_users():
	data={}
	data["flag"]="10"
	r = requests.get('http://127.0.0.1:80/api/v1/db/read', json=data)
	if(r.text=="0"):
		response = Response(response=json.dumps(dict(error='No content to send')),status=204, mimetype='application/json')
		return response
	
	
	return r.text


#API 9  Read from DB
@app.route('/api/v1/db/read', methods = ["POST","GET"])
def db_read():
	query=request.get_json()
	if(query["flag"]=="10"):
		result=user_table.query.filter_by().all()
		res={"users":[]}
				
		for r in result:
			res["users"].append(r.username)	
		if(len(res["users"])==0):
			return "0"
		return make_response(dumps(res["users"]))
					


#API 11 delete all users

@app.route('/api/v1/db/clear', methods = ["POST"])
def clear_db():
	del_entry=user_table.query.filter_by().all()
	deletions=[]
	for r in del_entry:
		deletions.append(r.username)
	for r in del_entry:
		db.session.delete(r)
		db.session.commit()
	
	response = Response(response=json.dumps(dict()),status=200, mimetype='application/json')
	return response



if __name__ == "__main__":
	db.create_all()
	app.run(debug = True, host = '0.0.0.0', port = 80)

