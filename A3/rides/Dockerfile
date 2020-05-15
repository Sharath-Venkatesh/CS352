FROM python:3.6-alpine
WORKDIR /app
COPY . /app
EXPOSE 80
ENV TEAM_NAME=CC_1114_1411_1727
RUN pip install -r requirements.txt
CMD ["python", "app.py"]
