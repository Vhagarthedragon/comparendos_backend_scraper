FROM python:3.9
ENV PYTHONUNBUFFERED 1

RUN mkdir /code
WORKDIR /code
COPY . /code

RUN pip install --upgrade pip
RUN pip install -r ./requirements.txt
RUN pip install boto3

COPY ./entrypoint.sh /
ENTRYPOINT ["sh", "/entrypoint.sh"]
