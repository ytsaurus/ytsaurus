"""
Pretty printers for arcadia types.

Arcadia types supported:
 TVector
 TString
 TUtf16String
 TBasicStringBuf
 THashMap
 THashSet
 THashMultiMap
 NWRE::TSubmatch
 TBitMap
 TBitMapOps
 TEnumBitSet
 TSfEnumBitSet
 TGramBitSet
 NActors::TActorId
"""

import gdb
import itertools
import re
import sys

from libstdcpp_printers import imap, izip, Iterator, Printer, \
    print_type, _string_encoding, _is_python2

from libcxx_printers import StdPairPrinter, StdVectorPrinter, FilteringTypePrinter, StdStringPrinter

from arcadia_helpers import hasCowInString

class PtrPrinter:
    'Print an Arcadia smart pointer'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def children(self):
        return [('Get()', self.val['T_'])]

    def to_string(self):
        return '%s<%s>' % (self.typename, print_type(self.val['T_'].type.target()))

class CowPtrPrinter:
    'Print a CowPtr'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        return '%s = %s' % (self.typename, self.val['T_'])

class TMaybePrinter:
    'Print a TMaybe'

    def __init__(self, typename, val):
        self.typename = typename
        if val['Defined_']:
            self.val = val['Data_'].address.dereference()
        else:
            self.val = None

    def children(self):
        return [] if self.val is None else [('GetRef()', self.val)]

    def to_string(self):
        return 'TMaybe' + (' empty' if self.val is None else '')


class THashMapPrinter(object):
    'Print THashMap'

    class _iterator(Iterator):
        def __init__(self, container, buckets):
            self.container = container
            self.count = 0
            self.item = None
            bucket_count = int(buckets['Size']['Divisor'])
            if bucket_count > 0:
                bucket = buckets['Data']
                for i in range(0, bucket_count):
                    if bucket.dereference() != 0:
                        self.item = bucket.dereference()
                        break
                    bucket = bucket + 1

        def __iter__(self):
            return self

        def __next__(self):
            if not self.item:
                raise StopIteration
            cnt = self.count
            val = self.item.dereference()['val']

            self.item = self.item.dereference()['next']
            self.count = self.count + 1
            longlong = gdb.lookup_type('long long')
            if self.item.cast(longlong) % 2 != 0:
                bucket = (self.item.cast(longlong) - 1).cast(self.item.type.pointer())
                while bucket.dereference() == 0:
                    bucket = bucket + 1
                if bucket.dereference().cast(longlong) % 2 != 0:
                    self.item = (bucket.dereference().cast(longlong) - 1).cast(self.item.type)
                else:
                    self.item = bucket.dereference()

            return self.container.display_value(cnt, val)

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        return('%s of length %d' % (
            self.typename, self.val['rep']['num_elements']))

    def display_value(self, cnt, val):
        return ('[%s] %s' % (cnt, val['first']), val['second'])

    def children(self):
        return self._iterator(self, self.val['rep']['buckets'])

    def display_hint(self):
        return 'THashMap'


class THashSetPrinter(THashMapPrinter):
    'Print THashSet'

    def __init__(self, typename, val):
        super(THashSetPrinter, self).__init__(typename, val)

    def display_value(self, cnt, val):
        return ('[%s]' % cnt, val)

    def display_hint(self):
        return 'THashSet'


class TStringPrinter:
    'Print a TString'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        # Make sure &string works, too.
        type = self.val.type
        if type.code == gdb.TYPE_CODE_REF:
            type = type.target()

        if 'Storage_' in type.keys() or 'TBasicStringStorage<char, std::__y1::char_traits<char>, true>' in type.keys():
            std_str = self.val['Storage_']
            return StdStringPrinter(std_str.type, std_str).to_string()

        if hasCowInString(type):
            std_str = self.val['S_']['T_'].dereference()
            return StdStringPrinter(std_str.type, std_str).to_string()

        ptr = self.val['Data_']
        return ptr.string(_string_encoding, 'replace')

    def display_hint(self):
        return 'string'


class TStringBufPrinter:
    'Print a TStringBuf'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        sv_type = gdb.lookup_type('std::__y1::string_view')
        return self.val.cast(sv_type)

    def display_hint(self):
        return 'string'


class TUtf16StringPrinter:
    'Print a TUtf16String'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        type = self.val.type
        if type.code == gdb.TYPE_CODE_REF:
            type = type.target()

        if hasCowInString(type, 'char16_t'):
            std_str = self.val['S_']['T_'].dereference()
            return 'L"' + StdStringPrinter(std_str.type, std_str).to_string() + '"'
        ptr = self.val['Data_']
        if _is_python2:
            r = unicode()
            while ptr.dereference() != 0:
                r += unichr(int(ptr.dereference()))
                ptr = ptr + 1
            r = r.encode(_string_encoding, 'replace')
        else:
            r = str()
            while ptr.dereference() != 0:
                r += chr(int(ptr.dereference()))
                ptr = ptr + 1
        return 'L"' + r + '"'

    def display_hint(self):
        return 'TUtf16String'


class TGUIDPrinter:
    'Print a TGUID'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        dw = self.val['dw']
        return('%x-%x-%x-%x' % (dw[0], dw[1], dw[2], dw[3]))

    def display_hint(self):
        return 'string'

# shamelessly stolen from https://svn.apache.org/repos/asf/subversion/trunk/tools/dev/gdb-py/svndbg/printers.py
def children_as_map(children_iterator):
    ''' Convert an iteration of (key, value) pairs into the form required for a
        pretty-printer 'children' method when the display-hint is 'map'.  '''
    for k, v in children_iterator:
        yield 'key', k
        yield 'val', v

class TRbTreePrinter:
    'Print TRbTree'

    class _iter(Iterator):
        def __init__(self, node, end, valtype):
            self.node = node
            self.end = end
            self.valtype = valtype

        def __iter__(self):
            return self

        @staticmethod
        def MinimumNode(node):
            while node['Left_'] != 0:
                node = node['Left_']
            return node

        def __next__(self):
            if self.node == self.end:
                raise StopIteration
            retval = self.node
            # self.node contains `next` value
            if self.node['Right_'] != 0:
                self.node = self.MinimumNode(self.node)
            else:
                y = self.node['Parent_']
                while self.node == y['Right_']:
                    self.node = y
                    y = y['Parent_']
                if self.node['Right_'] != y:
                    self.node = y
            return ('(%s*) %s' % (print_type(self.valtype), retval.cast(self.valtype.pointer())), retval.dereference().cast(self.valtype))

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def begin(self):
        return self.val['Data_']['Left_']

    def end(self):
        return self.val['Data_'].address

    def children(self):
        valtype = self.val.type.template_argument(0)
        return children_as_map(self._iter(self.begin(), self.end(), valtype))

    def to_string(self):
        if self.begin() == self.end():
            return 'empty %s' % (self.typename)
        return '%s' % self.typename

    def display_hint(self):
        return 'map'


class TIntrusiveListPrinter:
    'Print TIntrusiveList'

    class _iter(Iterator):
        def __init__(self, node, end, valtype):
            self.node = node
            self.end = end
            self.valtype = valtype

        def __iter__(self):
            return self

        def __next__(self):
            if self.node == self.end:
                raise StopIteration
            retval = self.node
            self.node = self.node['Next_']
            return ('(%s*) %s' % (print_type(self.valtype), retval.cast(self.valtype.pointer())), retval.dereference().cast(self.valtype))

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def begin(self):
        return self.val['End_']['Next_']

    def end(self):
        return self.val['End_'].address

    def children(self):
        valtype = self.val.type.template_argument(0)
        return children_as_map(self._iter(self.begin(), self.end(), valtype))

    def to_string(self):
        if self.begin() == self.end():
            return 'empty %s' % (self.typename)
        return '%s' % (self.typename)

    def display_hint(self):
        return 'map'


class TBitMapPrinter:
    'Print a TBitMap'

    class _iterator(Iterator):
        def __init__(self, start, size):
            self.item = start
            self.size = size
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.count >= self.size:
                raise StopIteration
            count = self.count
            if self.item.type.code == gdb.TYPE_CODE_ARRAY:
                elt = self.item[count]
            else:
                elt = self.item.dereference()
                self.item = self.item + 1
            self.count = self.count + 1
            return('[%d]' % count, elt)

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def children(self):
        return self._iterator(self.val['Mask']['Data'],
                              self.val['Mask']['Size'])

    def to_string(self):
        return('%s of length %d' % (self.typename, self.val['Mask']['Size']))

    def display_hint(self):
        return 'array'


class TEnumBitSetPrinter:
    'Print a TEnumBitSet'

    def __init__(self, typename, val):
        self.typename = typename
        type = val.type.unqualified().strip_typedefs()
        if type.code == gdb.TYPE_CODE_REF:
            type = type.target().unqualified().strip_typedefs()
        try:
            self.etype = type.template_argument(0)
        except:
            for f in type.fields():
                if f.is_base_class:
                    type = f.type
                    break
            self.etype = type.template_argument(0)
        self.offset = int(val['BeginIndex'])
        self.val = val

    def to_string(self):
        # If template_argument handled values, we could print the
        # size.  Or we could use a regexp on the type.
        return '%s' % (self.typename)

    def children(self):
        words = self.val['Mask']['Data']
        wtype = words.type

        # The _M_w member can be either an unsigned long, or an
        # array.  This depends on the template specialization used.
        # If it is a single long, convert to a single element list.
        if wtype.code == gdb.TYPE_CODE_ARRAY:
            tsize = wtype.target().sizeof
        else:
            words = [words]
            tsize = wtype.sizeof

        nwords = self.val['Mask']['Size']
        result = []
        byte = 0
        count = 0
        while byte < nwords:
            w = words[byte]
            bit = 0
            while w != 0:
                if (w & 1) != 0:
                    val = byte * tsize * 8 + bit
                    result.append((
                        '[%d]' % count,
                        gdb.Value(val + self.offset).cast(self.etype)))
                    count += 1
                bit = bit + 1
                w = w >> 1
            byte = byte + 1
        return result.__iter__()

    def display_hint(self):
        return 'array'


class TContBtFunction(gdb.Command):
    reg_x86_64 = 'rbx rbp r12 r13 r14 r15 rsp pc'.split()

    def __init__(self):
        super(TContBtFunction, self).__init__('bt-tcont', gdb.COMMAND_USER)

    def invoke(self, value, from_tty):
        regsize = gdb.parse_and_eval('$pc').type.sizeof
        if regsize != 8:
            raise NotImplementedError('Unexpected sizeof $pc', regsize)

        self.TCont = gdb.lookup_type('TCont')
        self.TContRep = gdb.lookup_type('TContRep')
        self.TContMachineContext = gdb.lookup_type('TContMachineContext')

        value = gdb.parse_and_eval(value)
        if value.type == self.TCont.pointer():
            value = value.dereference()
        if value.type == self.TCont:
            value = value['Rep_']
        if value.type == self.TContRep.pointer():
            value = value.dereference()
        if value.type == self.TContRep:
            ctxlen = int(value['machine']['Len'])
            if ctxlen < len(self.reg_x86_64) * regsize:
                raise NotImplementedError('Unexpected machine context length', ctxlen)
            value = value['machine']['DataPtr'].cast(self.TContMachineContext.pointer()).dereference()
        if value.type != self.TContMachineContext:
            raise TypeError('Can backtrace only TCont, TContRep and TContMachineContext')
        # casting is required as $pc has type `pointer-to-function`
        oldctx = {reg: gdb.parse_and_eval('(uint64_t)$' + reg) for reg in self.reg_x86_64}
        corctx = {reg: value['Buf_'][ndx] for ndx, reg in enumerate(self.reg_x86_64)}
        oldctx = ', '.join('$%s = 0x%x' % (reg, oldctx[reg]) for reg in self.reg_x86_64)
        corctx = ', '.join('$%s = 0x%x' % (reg, corctx[reg]) for reg in self.reg_x86_64)
        print('current:  ' + oldctx)
        print('TCont...: ' + corctx)
        try:
            # `frame $rsp $pc` does not work properly, so `bt' works iff the process is alive.
            # See https://www.sourceware.org/ml/gdb/2011-02/msg00042.html
            gdb.execute('set ' + corctx)
            gdb.execute('bt')
        finally:
            gdb.execute('set ' + oldctx)


class TActorIdPrinter:
    'Print a NActors::TActorId'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        return ('[%u:%lu:%u]' % (self.node_id_val(), self.local_id_val(), self.hint_val()))

    def node_id_val(self):
        return self.val['Raw']['N']['NodeId'] & self.val['NodeIdMask']

    def local_id_val(self):
        return self.val['Raw']['N']['LocalId']

    def hint_val(self):
        return self.val['Raw']['N']['Hint']

    def display_hint(self):
        return 'TActorId'


def build_printer():
    printer = Printer('util')
    printer.add('TString', TStringPrinter)  # <r5448720
    printer.add('TBasicString<char, TCharTraits<char> >', TStringPrinter)  # <r7721263
    printer.add('TBasicString<char, std::__y1::char_traits<char> >', TStringPrinter)  # >=r7721263
    printer.add('TBasicString<char, std::__y1::char_traits<char>, false>', TStringPrinter)
    printer.add('TBasicString<char, std::char_traits<char> >', TStringPrinter)
    printer.add('TUtf16String', TUtf16StringPrinter)  # <r5448720
    printer.add('TBasicString<unsigned short, TCharTraits<unsigned short> >', TUtf16StringPrinter)  # <r5520905
    printer.add('TBasicString<char16_t, TCharTraits<char16_t> >', TUtf16StringPrinter)  # <r7339064
    printer.add('TBasicString<char16_t, TCharTraits<wchar16> >', TUtf16StringPrinter) # <r7721263
    printer.add('TBasicString<char16_t, std::__y1::char_traits<char16_t> >', TUtf16StringPrinter) # >=r7721263
    printer.add('TBasicString<char16_t, std::__y1::char_traits<char16_t>, false>', TUtf16StringPrinter) # >=r7721263
    printer.add('TBasicString<char16_t, std::char_traits<char16_t> >', TUtf16StringPrinter)
    printer.add('TBasicStringBuf', TStringBufPrinter)
    printer.add('TMsString', TStringBufPrinter)
    printer.add('TVector', StdVectorPrinter)
    printer.add('THashMap', THashMapPrinter)
    printer.add('THashSet', THashSetPrinter)
    printer.add('THashMultiMap', THashMapPrinter)
    printer.add('NWRE::TSubmatch', StdPairPrinter)
    printer.add('TBitMap', TBitMapPrinter)
    printer.add('TBitMapOps', TBitMapPrinter)
    printer.add('TEnumBitSet', TEnumBitSetPrinter)
    printer.add('TSfEnumBitSet', TEnumBitSetPrinter)
    printer.add('TGramBitSet', TEnumBitSetPrinter)
    printer.add('TGUID', TGUIDPrinter)
    printer.add('TIntrusivePtr', PtrPrinter)
    printer.add('TIntrusiveConstPtr', PtrPrinter)
    printer.add('TIntrusiveList', TIntrusiveListPrinter)
    printer.add('TRbTree', TRbTreePrinter)
    printer.add('TAutoPtr', PtrPrinter)
    printer.add('THolder', PtrPrinter)
    printer.add('TCowPtr', CowPtrPrinter)
    printer.add('TCopyPtr', PtrPrinter)
    printer.add('TSharedPtr', PtrPrinter)
    printer.add('TAtomicSharedPtr', PtrPrinter)
    printer.add('TSimpleSharedPtr', PtrPrinter)
    printer.add('TMaybe', TMaybePrinter)
    printer.add('NActors::TActorId', TActorIdPrinter)
    return printer


def register_printers():
    gdb.printing.register_pretty_printer(None, build_printer())
    for substring, shortname in [
        ('TBasicString', 'TString'),
        ('TBasicString', 'TUtf16String'),
        ('TBasicString', 'TUtf32String'),
    ]:
        gdb.types.register_type_printer(None, FilteringTypePrinter(substring, shortname))
