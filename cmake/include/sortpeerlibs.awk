#!/bin/awk

BEGIN {
	# parse peerdir variable
	{
		while (length(peerdir) > 0) {
			p = peerdir;
			if (p ~ /\;/) {
				sub(/^[^\;]+\;/, "", peerdir);
				p = substr(p, 1, length(p) - length(peerdir) - 1);
			} else {
				peerdir = "";
			}
			if (length(statfile) > 0) {
				print p "\n" >> statfile;
			}
			
			peerdirs[p] = 1;
		}
	}
}

{
	if (peerdirs[$0] > 0) {
		ordered = ordered " " $0;
		peerdirs[$0] = 2;
	}
}

END {
	# check if all of peerdirs have been reordered
	for (i in peerdirs) {
		if (peerdirs[i] == 1) {
			ordered = ordered " " i;
			absent = absent " " i;
		}
	}
	print ordered;
	print absent > "/dev/stderr";
}
