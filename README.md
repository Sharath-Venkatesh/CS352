# Final Project: Mini DBaaS for Rideshare
###  17CS352 Cloud Computing, PES University
&nbsp;
The final project is focused on building a fault tolerant, highly available database as a service for
the RideShare application. You will only be working with Amazon EC2 instances.
---
##### Architecture of DBaas:
  - ALB
  - Users Instance
  - Rides Instance
  - Orchestrator
  -- Includes Master and Slave Nodes

# How to run
- Users Instance. CD into users folder and execute:
```sh
$ sudo docker build -t users:latest .
$ sudo docker run --name users -p 80:80 users:latest
```
- Ride Instance. CD into rides folder and execute:
```sh
$ sudo docker build -t rides:latest .
$ sudo docker run --name rides -p 80:80 rides:latest
```
- Orchestrator
```sh
$ sudo docker-compose up --build
```
### To remove containers and images
```sh
$ sudo docker rm -vf $(sudo docker ps -a -q)
$ sudo docker rmi -f $(sudo docker images -a -q)
```
