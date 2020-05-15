FROM python:3.8-alpine
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
WORKDIR /app
COPY . /app
RUN pip install docker
RUN pip install flask
RUN pip install pika
RUN pip install kazoo
RUN pip install requests
RUN pip install SQLAlchemy
RUN pip install Flask-SQLAlchemy
CMD ["python3", "-u", "worker.py"]
