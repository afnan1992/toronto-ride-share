FROM python:3.9.5

# set up location of code

WORKDIR /code
RUN mkdir /code/data/


# install python requirements
COPY /containers/extract/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY /containers/extract/config.cfg config.cfg

RUN mkdir /code/data/extract/

# COPY our code folder to docker
COPY ./src /code

# keep our docker container running
CMD ["tail", "-f", "/dev/null"]