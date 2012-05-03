#!/usr/bin/python

from ansi import Fore, Style

import sys

bold = {
        'D': Style.DIM,
        'W': Style.BRIGHT,
        'E': Style.BRIGHT,
        'I': Style.BRIGHT
        }

color = {
        'D': Fore.WHITE, 
        'W': Fore.YELLOW, 
        'E': Fore.RED
        }

for line in sys.stdin:
    line = line.strip('\n')
    fields = line.split('\t')

    if len(fields) < 3:
        print line
        continue

    date = fields[0]
    mode = fields[1]
    if mode in color:
        date = color[mode] + date + Fore.RESET
    
    message = '\t'.join([date] + fields[1:])
    if mode in bold:
        message = bold[mode] + message + Style.RESET_ALL
    
    print message
