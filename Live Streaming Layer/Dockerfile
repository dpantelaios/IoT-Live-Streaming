FROM python:latest

WORKDIR /usr/app/src

COPY requirements.txt ./
COPY consumer.py ./

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python3", "-u", "./consumer.py"]