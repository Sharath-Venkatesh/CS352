# Final Project: RideShare App: Database as a Service
###  17CS352 Cloud Computing, PES University
&nbsp;
This project develops a fault-tolerant and highly available Database as a Service (DBaaS) tailored for the RideShare application, ensuring reliable, real-time functionality.
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
