FROM python:3.8-alpine
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
WORKDIR /app
COPY . /app
EXPOSE 80
RUN pip install docker
RUN pip install docker-compose==1.12.0
RUN pip install flask
RUN pip install kazoo
RUN pip install pika
RUN pip install requests
RUN pip install SQLAlchemy
RUN pip install Flask-SQLAlchemy
