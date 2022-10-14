FROM python:3.8.0

RUN mkdir /project
WORKDIR /project

COPY ./resources/requirements.txt ./
ADD ./resources/. /resources/
ADD ./app/. /app/

RUN pip install -r ./requirements.txt

CMD [ "python", "/app/main.py" ]
