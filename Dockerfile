FROM postgres:14.6
ENV POSTGRES_PASSWORD postgres 
ENV POSTGRES_DB searchdb
COPY ./init/ /docker-entrypoint-initdb.d/