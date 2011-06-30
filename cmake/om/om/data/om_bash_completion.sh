if [ "`basename $SHELL`" = bash ]; then
	complete -C 'om --complete' -o plusdirs om
fi
