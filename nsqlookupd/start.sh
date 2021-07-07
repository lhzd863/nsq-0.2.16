nohup ./nsqlookupd \
  -debug=false \
  -http-address="0.0.0.0:4161" \
  -tcp-address="0.0.0.0:4160" \
  -version=false > run.log 2>&1 &


