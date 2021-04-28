FROM amazoncorretto:8 as build-stage

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv git

RUN python -V
RUN python3 -V

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

RUN pip3 install --upgrade pip

RUN pip3 install pex==2.1.39 pyspark==3.0.1

RUN mkdir /ingestion
WORKDIR /ingestion

COPY . /ingestion

RUN pex $(pip freeze) . -v --disable-cache -D datalakeingestion/docs --ignore-errors -o release/datalakeingestion.pex
FROM scratch AS result
COPY --from=build-stage /ingestion/release/datalakeingestion.pex .