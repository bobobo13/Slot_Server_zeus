#!/bin/bash
########################################################
## Shell Script to Build Docker Image
########################################################
if [ ! $1 ]
then
echo "[package_name] [image_name] [image_tag] [Dockerfile]"
exit
fi

relative_path="$(dirname "$BASH_SOURCE")"
docker_file_path="$( cd $relative_path; pwd -P)"
package_name=$1
image_name=$2
image_tag=${3:-latest}
docker_file_name=${4:-Dockerfile}

result=$( sudo docker ps -aq --filter ancestor=$image_name:$image_tag )
if [ $result ]
then
echo "container exists"
echo "sudo docker stop $result"
echo "sudo docker rm $result"
exit
else
echo "No such container"
fi

result=$( sudo docker images -q $image_name:$image_tag )
if [ $result ]
then
echo "image exists"
echo "sudo docker rmi $result"
exit
else
echo "No such image"
fi

#echo "delete output file"
echo "build the docker image"
sudo docker build --no-cache --build-arg pkg=$package_name --build-arg docker_file_path=$relative_path -t $image_name:$image_tag -f $docker_file_path/$docker_file_name .

# Build Cache
sudo docker builder prune -f
echo "build finish..."
