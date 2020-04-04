import os
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fibers
import printing
import yt_rct
import yt_table_client

def register_printers():
    printing.ya_register(None) 
    yt_table_client.ya_register(None) 
    print('[yt] YT GDB pretty-printers enabled')

register_printers()

