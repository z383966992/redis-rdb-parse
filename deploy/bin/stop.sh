#!/bin/bash
PIDFILE=parseRdb.pid

echo -n "Stopping rdb parse service ..."
cd /export/App/jim-rdb.jd.local

if [ ! -f "$PIDFILE" ]
then
 echo "could not find pid file $PIDFILE"
 pkill parseRdb
else
 kill -9 $(cat "$PIDFILE")
 if [ $? -eq 0 ]
 then
  echo STOPPED
 else
  pkill parseRdb
 fi
 /bin/rm -rf "$PIDFILE"
fi
exit 0
