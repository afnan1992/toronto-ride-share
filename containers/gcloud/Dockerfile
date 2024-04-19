FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine
RUN apk --update-cache add openjdk8-jre
RUN gcloud components install app-engine-java kubectl

COPY ./keys /code/keys
# # Set the environment variable
ENV GOOGLE_APPLICATION_CREDENTIALS=/code/keys/data-proc-admin.json

RUN gcloud auth activate-service-account --key-file=/code/keys/data-proc-admin.json

CMD ["tail", "-f", "/dev/null"]
