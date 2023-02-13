docker image build -t rts .
docker container run -d -p 5050:5050 rts
docker container exec -it 2dce97565 bash


docker image build -f Dockerfile-base -t awsm-rts-base .


scp -i /c/work/AWSM-CICD-TEMP.pem ./* ec2-user@10.83.133.71:~/app

# Run from a mahcine that does not have python but has docker
docker container run -v /c/work/awsm/rts:/app -it python:3.8.12-bullseye bash
