#!/bin/bash
PIDFILE=parseRdb.pid

cd /export/App/jim-rdb.jd.local
chmod +x parseRdb
setsid ./parseRdb &

if [ $? -eq 0 ]
then
 if /bin/echo -n $! > "$PIDFILE"
 then
    sleep 1
    echo STARTED
 else
    echo FAILED TO WRITE PID
    exit 1
 fi
else
 echo SERVER DID NOT START
 exit 1
fi
