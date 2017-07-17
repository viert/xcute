FROM python:2.7
MAINTAINER Mikhail Petrov <azalio@azalio.net>

WORKDIR /usr/local/x
COPY . .
RUN pip install .
WORKDIR /root/
CMD /usr/local/bin/x
