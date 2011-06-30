#!/bin/sh
export TMPDIR=/tmp
. /etc/rc.conf.local
case $1 in
	restart|start)
		echo "(Re)starting..."
		killall distccd
		rm -r $TMPDIR/distcc*
		distccd  --allow 213.180.0.0/16 --listen $ipaddr --daemon;;
	stop)
		echo "Stopping..."
		killall distccd;;
	cleantmp)
		find $TMPDIR -mmin +10 -and -name "distcc*" | xargs rm -r;;
	*)
		echo "use " $0 " start|stop|restart";;
		
esac
