import os
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import libstdcpp_printers
import arcadia_printers
import libc_printers
import libcxx_printers
import libpython_printers
import yabs_printers

import arcadia_xmethods
import libcxx_xmethods

import yt_fibers_printer

import tcont_printer

def register_printers():
    libc_printers.register_printers()

    libcxx_printers.register_libcxx_printers(None)

    libstdcpp_printers.register_printers(None)

    arcadia_printers.register_printers()
    arcadia_printers.TContBtFunction()

    arcadia_xmethods.register_xmethods()
    libcxx_xmethods.register_xmethods()

    yabs_printers.YabsEnable()  # 'yabs-enable' command will register YaBS pretty-printers

    yt_fibers_printer.register_fibers_printer()

    tcont_printer.register_commands()

    print('[arc] Arcadia GDB pretty-printers enabled')

register_printers()
