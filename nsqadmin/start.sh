nohup ./nsqadmin \
  -graphite-url="" \
  -http-address="0.0.0.0:4171" \
  -nsqd-http-address=[] \
  -proxy-graphite=true \
  -template-dir="templates" \
  -use-statsd-prefixes=true \
  -version=false  > run.log 2>&1 &

