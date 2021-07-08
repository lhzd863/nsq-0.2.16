#nohup ./bench_reader --nsqd-tcp-address=127.0.0.1:4150 --topic=test --channel=test > run.log 2>&1 &
./bench_reader --nsqd-tcp-address=127.0.0.1:4160 --topic=test --channel=test

