FROM python:latest

WORKDIR /usr/app/src

COPY requirements.txt ./
COPY generate_data.py ./

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python3", "./generate_data.py"]