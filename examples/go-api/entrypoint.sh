#!/bin/sh
mkdir -p /data
rm -f /data/ring.dat
/usr/local/bin/zhensegg-broker \
    --addr 127.0.0.1:9090 --http-addr 0.0.0.0:9091 \
    --mode file --file /data/ring.dat \
    --ring-capacity-mb "${RING_MB:-1024}" --cores "${CORES:-4}" \
    ${OVERFLOW:+--on-overflow "$OVERFLOW"} &
sleep 1
exec /usr/local/bin/zhensegg-api
