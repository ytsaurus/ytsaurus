# -*- coding: utf-8 -*-

from os import getenv
from os.path import abspath, join, expanduser, expandvars, isfile, dirname
from re import compile

from util import map_scat
from exceptions import Error

class Item(object):
    stop_mark = '.'
    include_prefix = '+'
    comment_prefixes = [ '#', '//' ]
    carry_suffix = '\\'
    assign_op = '='
    append_op = '+='
    assign_if_op = '?='

    (not_ready, not_valuable, is_error, is_stop, is_include,
     is_expression) = range(6)

    def __init__(self):
        self.s = str()
    def take_line(self, line):
        map_scat(self.__dict__, 's', line.strip())
        if self.s.endswith(self.carry_suffix):
            self.s = self.s[0 : -1].rstrip()
            return self.not_ready
        return self.parse()

    def parse(self):
        if not self.s or (self.s.startswith(self.comment_prefixes[0]) or
                          self.s.startswith(self.comment_prefixes[1])):
            return self.not_valuable
        if self.s == self.stop_mark:
            return self.is_stop
        if (self.s.startswith(self.include_prefix)):
            self.include_path = self.s[len(self.include_prefix) : ].lstrip()
            return self.is_include
        operation, index = self.find_operation()
        if index < 1:
            return self.is_error
        self.name = self.s[ : index].strip()
        self.value = self.s[index + len(operation) : ].strip()
        self.operation = operation
        return self.is_expression
    def first_operation(self, op1, op2):
        index1 = self.s.find(op1)
        index2 = self.s.find(op2)
        if index1 == -1 or index2 != -1 and index2 < index1:
            return (op2, index2)
        else:
            return (op1, index1)
    def find_operation(self):
        operation = self.first_operation(self.assign_op, self.append_op)[0]
        return self.first_operation(operation, self.assign_if_op)

    def set_value(self, map, adjust):
        self.value = adjust(self.name, self.value)
        if self.operation == self.assign_op:
            map[self.name] = self.value
        elif self.operation == self.append_op:
            map_scat(map, self.name, self.value)
        elif (self.operation == self.assign_if_op and
              not map.has_key(self.name)):
            map[self.name] = self.value
    
class Functions(object):
    def __init__(self, dir, map):
        self.dir, self.map = dir, map
        self.re = compile(r'\b(\w+)\(([^)]+)\)')
    def __call__(self, name, value):
        first = 0
        new_value = ''
        for match in self.re.finditer(value):
            fn = match.group(1)
            arg = match.group(2)
            if fn == 'path':
                result = abspath(join(self.dir,
                                      expandvars(expanduser(arg))))
            elif fn == 'env':
                result = getenv(arg, '')
            elif fn == 'var':
                result = self.map.get(arg, '')
            new_value += value[first : match.start()]
            new_value += result
            first = match.end()
        return new_value + value[first : ]

class File(object):
    def __init__(self, path, base_dir=None):
        self.path, self.base_dir = path, base_dir
    # возвращает False если надо прекратить чтение конфигурации и True если нет
    def read(self, map):
        if not isfile(self.path):
            raise Error, ('path \'%s\' is not an existing regular file' %
                          self.path)

        item = Item()
        for i, line in enumerate(open(self.path)):
            res = item.take_line(line)
            if res == Item.not_ready:
                continue
            elif res == Item.is_stop:
                return False
            elif res == Item.is_include:
                if not File(join(dirname(self.path), item.include_path),
                            self.base_dir).read(map):
                    return False
            elif res == Item.is_expression:
                item.set_value(map, Functions(self.base_dir or
                                              dirname(self.path), map))
            elif res == Item.is_error:
                raise Error, ('configuration syntax error at %s:%d' %
                              (self.path, i + 1))
            # остался только случай Item.not_valuable
            item = Item()
    
        return True
