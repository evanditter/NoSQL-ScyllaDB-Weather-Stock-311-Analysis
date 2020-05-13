# pushed to aws ECR
# aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 295274106073.dkr.ecr.us-east-2.amazonaws.com/sld
# docker build -t sld .
# docker tag sld:latest 295274106073.dkr.ecr.us-east-2.amazonaws.com/sld:latest
# docker push 295274106073.dkr.ecr.us-east-2.amazonaws.com/sld:latest

FROM python:3 as build

RUN pip install cassandra-driver pandas docopt

FROM build

ADD sld.py .
ADD models.py .

ENTRYPOINT ["python", "./sld.py"]