import gdb
import ipaddress
import socket

from libstdcpp_printers import Printer

class TIpv4Printer:
    'Print struct in_addr'

    def __init__(self, typename, val: gdb.Value):
        self.val = val

    def to_string(self):
        s_addr = self.val['s_addr']
        addr_int = socket.ntohl(int(s_addr))

        return str(ipaddress.IPv4Address(addr_int))


class TIpv6Printer:
    'Print struct in6_addr'

    def __init__(self, typename, val: gdb.Value):
        self.val = val

    def to_string(self):
        s6_addr = self.val['__in6_u']['__u6_addr8']
        addr_bytes = bytes(int(s6_addr[i]) for i in range(16))

        return str(ipaddress.IPv6Address(addr_bytes))


class TSockaddrPrinter:
    'Print struct sockaddr_*'

    def __init__(self, typename, val: gdb.Value):
        if typename == 'sockaddr_storage':
            family = int(val['ss_family'])
            if family == socket.AF_INET.value:
                val = val.cast(gdb.lookup_type('sockaddr_in'))
            elif family == socket.AF_INET6.value:
                val = val.cast(gdb.lookup_type('sockaddr_in6'))
        self.val = val
        self.typename = val.type.name

    def to_string(self):
        if self.typename == 'sockaddr_in':
            addr = self.val['sin_addr']
            port = socket.ntohs(int(self.val['sin_port']))
            return f'IPv4({addr}:{port})'
        elif self.typename == 'sockaddr_in6':
            addr = str(self.val['sin6_addr'])
            scope_id = int(self.val['sin6_scope_id'])
            if scope_id:
                addr += f'%{scope_id}'
            port = socket.ntohs(int(self.val['sin6_port']))

            extra = ''
            flow_id = int(self.val['sin6_flowinfo'])
            if flow_id:
                extra = f', flow_id = {flow_id}'

            return f'IPv6([{addr}]:{port}{extra})'
        return self.val.format_string(raw=True)


def build_printer():
    printer = Printer('libc')
    printer.add('in6_addr', TIpv6Printer)
    printer.add('in_addr', TIpv4Printer)
    printer.add('sockaddr_in', TSockaddrPrinter)
    printer.add('sockaddr_in6', TSockaddrPrinter)
    printer.add('sockaddr_storage', TSockaddrPrinter)
    return printer


def register_printers():
    gdb.printing.register_pretty_printer(None, build_printer())
