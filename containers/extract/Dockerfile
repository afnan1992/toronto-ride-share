FROM python:3.9.5

# set up location of code

WORKDIR /code
RUN mkdir -p /code/data
RUN mkdir -p /code/data/extract/
RUN mkdir -p /code/data/extract/weather_data/

COPY ./src /code


# install python requirements
COPY /containers/extract/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY /containers/extract/config.cfg config.cfg



COPY ./keys /code/keys
# # Set the environment variable
#ENV GOOGLE_APPLICATION_CREDENTIALS=/code/keys/key-1.json

# keep our docker container running
CMD ["tail", "-f", "/dev/null"]