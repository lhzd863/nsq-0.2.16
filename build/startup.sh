#
nohup ./nsqlookupd > run-look.log 2>&1 &
#
nohup ./nsqd --lookupd-tcp-address=192.168.1.189:4160 > run-nsq.log 2>&1 &
#
nohup ./nsqadmin --lookupd-http-address=192.168.1.189:4161 --template-dir=./templates >run-admin.log 2>&1 &

