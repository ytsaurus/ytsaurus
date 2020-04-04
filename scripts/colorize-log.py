#!/usr/bin/env python

#code was taken from http://code.google.com/p/colorama/

'''
Copyright (c) 2010 Jonathan Hartley <tartley@tartley.com>

Released under the New BSD license (reproduced below), or alternatively you may
use this software under any OSI approved open source license such as those at
http://opensource.org/licenses/alphabetical

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name(s) of the copyright holders, nor those of its contributors
  may be used to endorse or promote products derived from this software without
  specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''


'''
This module generates ANSI character codes to printing colors to terminals.
See: http://en.wikipedia.org/wiki/ANSI_escape_code
'''

CSI = '\033['

def code_to_chars(code):
    return CSI + str(code) + 'm'

class AnsiCodes(object):
    def __init__(self, codes):
        for name in dir(codes):
            if not name.startswith('_'):
                value = getattr(codes, name)
                setattr(self, name, code_to_chars(value))

class AnsiFore:
    BLACK   = 30
    RED     = 31
    GREEN   = 32
    YELLOW  = 33
    BLUE    = 34
    MAGENTA = 35
    CYAN    = 36
    WHITE   = 37
    RESET   = 39

class AnsiBack:
    BLACK   = 40
    RED     = 41
    GREEN   = 42
    YELLOW  = 43
    BLUE    = 44
    MAGENTA = 45
    CYAN    = 46
    WHITE   = 47
    RESET   = 49

class AnsiStyle:
    BRIGHT    = 1
    DIM       = 2
    NORMAL    = 22
    RESET_ALL = 0

Fore = AnsiCodes( AnsiFore )
Back = AnsiCodes( AnsiBack )
Style = AnsiCodes( AnsiStyle )

# until here
##################

# log structure:
# date mode cat msg file line func thread
# --date --mode --cat --msg --file --line --func --thread
# --all

import sys
import argparse

from collections import defaultdict

parser = argparse.ArgumentParser(description="Colorize log")
parser.add_argument("--all", action='store_true', default=False)
args = parser.parse_args()

show_all = args.all


bold = defaultdict(str, 
    {
        'D': Style.DIM,
        'W': Style.BRIGHT,
        'E': Style.BRIGHT,
        'I': Style.BRIGHT
    })

color = defaultdict(str,
    {
        'D': Fore.WHITE, 
        'W': Fore.YELLOW, 
        'E': Fore.RED
    })

def unescape_error(error):
    result = ""
    l = len(error)
    i = 0
    indent = 4 * ' '
    while i < l:
        if i + 1 < l and error[i] == '\\' and error[i + 1] == 'n':
            result += '\n' + indent
            i += 2
        else:
            result += error[i]
            i += 1
    return result


for line in sys.stdin:
    line = line.strip('\n')
    fields = line.split('\t')

    if len(fields) < 3:
        print line
        continue

    date = fields[0]
    mode = fields[1]
    fields[2] = fields[2].ljust(10)

    if show_all:
        shown_fields = fields[1:]
    else:
        shown_fields = fields[1:-4]

    message = '  '.join([date] + shown_fields)
    message = unescape_error(message)
    message = color[mode] + bold[mode] + message + Style.RESET_ALL
    
    print message
