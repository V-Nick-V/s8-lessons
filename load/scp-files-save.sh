#!/bin/bash

USER_VM=yc-user
HOST_VM=51.250.74.254
IMAGE_VM=$(ssh -i ssh_key $USER_VM@$HOST_VM docker ps --format '{{.Names}}')
echo USER_VM $USER_VM
echo HOST_VM $HOST_VM
echo IMAGE_VM $IMAGE_VM
ssh -i ssh_key $USER_VM@$HOST_VM "
    docker cp $IMAGE_VM:/lessons ~/
"
scp -ri ssh_key $USER_VM@$HOST_VM:~/lessons . 
