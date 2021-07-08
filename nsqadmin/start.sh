nohup ./nsqadmin \
  -graphite-url="" \
  --http-address="0.0.0.0:4171" \
  --lookupd-http-address="127.0.0.1:4161" \
  -proxy-graphite=true \
  -template-dir="templates" \
  -use-statsd-prefixes=true \
  -version=false  > run.log 2>&1 &

