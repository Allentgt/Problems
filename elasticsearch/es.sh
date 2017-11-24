#!/bin/bash -e
sudo yum update -y aws-cli
sudo yum install -y libffi-devel.x86_64
#sudo yum install -y gcc
sudo yum install -y python27-libs.x86_64

echo "**********************************************"
python -V
echo "**********************************************"

echo "I AM WAITING"
export PATH=$PATH:/mnt:/mnt1
echo "PATH DEAR!!!-----" $PATH

sudo pip install -U pyOpenSSL ndg-httpsclient pyasn1
sudo pip install boto3
sudo pip install simplejson
sudo pip install elasticsearch
sudo pip install -U requests
#sudo pip install -U requests[security]
sudo pip install requests-aws-sign
sudo pip install pandas==0.20.3
sudo pip install numpy
sudo pip install s3fs

echo "********************Download all necessary scripts********************"

aws s3 cp s3://temp-devops/GPS-PathFinder/uat/Setupfiles/fileList.py ./
aws s3 cp s3://temp-devops/GPS-PathFinder/uat/Setupfiles/index.py ./
aws s3 cp s3://temp-devops/GPS-PathFinder/uat/Setupfiles/bulk.py ./
aws s3 cp s3://temp-devops/GPS-PathFinder/uat/Setupfiles/finish.py ./

df

echo "********************Create index with mapping********************"

index=`python index.py $1 $2 $3`
echo $index
cnt=0
filelist=`python fileList.py $1`
echo $filelist
o=1
for file in $filelist
do

	echo $file
	aws s3 cp $file /mnt/
	
	c=1
	n=1

	FILENAME=/mnt/${file##*/}
	WC=$(cat $FILENAME | wc -l)
	splitr=$(($WC/10))
	HDR=$(head -1 $FILENAME)
	split -l $splitr $FILENAME /dev/shm/xyz${o}
	rm -f $FILENAME
	((cnt=cnt+WC-1))
	for f in /dev/shm/xyz${o}*
	do
   		echo $HDR > /dev/shm/Part${o}-${n}
   		cat $f >> /dev/shm/Part${o}-${n}
   		rm -f $f
   		((n++))
	done

	sed -i '1d' /dev/shm/Part${o}-1

	for f in /dev/shm/Part${o}-*
	do
        	echo $1 $2 $index $f ${o}${c}00000000 $file
        	nohup python bulk.py $1 $2 $index $f ${o}${c}00000000 $file >>nohup${c}.out 2>&1 &
		echo $! >> pid
        	((c++))
	done

	while true;
	do
    	if [ -s pid ] ; then
        	for pid in `cat pid`
        	do
            		kill -0 "$pid" 2>/dev/null || sed -i "/^$pid$/d" pid
        	done
    	else
        	echo "All your process completed"
        	break
    	fi
	done

	rm -f /dev/shm/Part* 

	((o++))	
done

python finish.py $1 $2 $index $cnt

