#!/bin/bash -xe

sudo yum install -y python-numpy python-pip python-matplotlib tmux

sudo pip install jupyter pandas astropy esutil awscli

if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then

sudo yum install -y git libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel
cd /tmp
git clone https://github.com/s3fs-fuse/s3fs-fuse
cd s3fs-fuse/
./autogen.sh
./configure --prefix=/usr --with-openssl
make
sudo make install

echo "s3fs#ctslater-spark /home/hadoop/s3 fuse _netdev,allow_other,iam_role=EMR_EC2_DefaultRole,uid=498,gid=500,use_cache='/tmp' 0 0" | sudo tee -a /etc/fstab
mkdir /home/hadoop/s3
sudo mount /home/hadoop/s3

aws s3 cp s3://ctslater-spark/kernel.json /tmp/kernel.json
sudo mkdir /usr/local/share/jupyter/kernels/pyspark/
sudo cp /tmp/kernel.json /usr/local/share/jupyter/kernels/pyspark/

fi
