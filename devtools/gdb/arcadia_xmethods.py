import gdb
import gdb.xmethod
import re
from libcxx_xmethods import StringSizeWorker, StringIndexWorker, StringDataWorker
from arcadia_helpers import hasCowInString

class PtrGetWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, pointer_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.pointer_type = pointer_type

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.pointer_type

    def __call__(self, obj):
        return obj['T_']

class PtrGetRefWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, pointer_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.pointer_type = pointer_type

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.pointer_type.dereference()

    def __call__(self, obj):
        return obj['T_'].dereference()


class PtrMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'arcadia::ptr')
        self.classes = ['TIntrusivePtr', 'TIntrusiveConstPtr', 'TAutoPtr',
                        'THolder', 'TSharedPtr', 'TAtomicSharedPtr', 'TSimpleSharedPtr']
        self.method_workers = {
            'Get': PtrGetWorker,
            'GetRef': PtrGetRefWorker,
            'operator->': PtrGetWorker,
            'operator*': PtrGetRefWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        found = False
        for cl in self.classes:
            if class_type.tag.startswith(cl + '<'):
                found = True
        if not found:
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            pointer_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, pointer_type)


class TMaybeGetWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.element_type.pointer()

    def __call__(self, obj):
        if obj['Defined_']:
            return obj['Data_'].address
        return gdb.parse_and_eval('(void*)0')

class TMaybeGetRefWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.element_type.reference()

    def __call__(self, obj):
        if obj['Defined_']:
            return obj['Data_'].reference_value()
        raise Exception('Trying to access data of empty TMaybe')


class TMaybeMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'arcadia::TMaybe')
        self.class_name = 'TMaybe'
        self.method_workers = {
            'Get': TMaybeGetWorker,
            'GetRef': TMaybeGetRefWorker,
            'operator->': TMaybeGetWorker,
            'operator*': TMaybeGetRefWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            element_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, element_type)


class TStringSizeWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type
        self.result_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.result_type

    def __call__(self, obj):
        if hasCowInString(self.class_type):
            std_str = obj['S_']['T_'].dereference()
            return StringSizeWorker(self.class_type, self.element_type)(std_str)
        return obj['Data_'].cast(gdb.lookup_type('NDetail::TStringData').pointer())[-1]['Length']


class TStringIndexWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.index_type = gdb.lookup_type('size_t')
        self.result_type = element_type.reference()

    def get_arg_types(self):
        return self.index_type

    def get_result_type(self, obj, idx):
        return self.result_type

    def __call__(self, obj, idx):
        if hasCowInString(self.class_type):
            std_str = obj['S_']['T_'].dereference()
            return StringIndexWorker(self.class_type, self.result_type.target())(std_str, idx)
        return obj['Data_'][idx].reference_value()


class TStringDataWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.result_type = element_type.pointer()

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.result_type

    def __call__(self, obj):
        if hasCowInString(self.class_type):
            std_str = obj['S_']['T_'].dereference()
            return StringDataWorker(self.class_type, self.result_type.target())(std_str)
        return obj['Data_'].cast(self.result_type)


class TStringCStrWorker(TStringDataWorker):
    def __init__(self, class_type, element_type):
        TStringDataWorker.__init__(self, class_type, element_type)
        self.result_type = element_type.const().pointer()


class TStringMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'arcadia::TString')
        self.class_name = 'TBasicString'
        self.method_workers = {
            'size': TStringSizeWorker,
            'length': TStringSizeWorker,
            'Size': TStringSizeWorker,
            'Length': TStringSizeWorker,
            'operator[]': TStringIndexWorker,
            'data': TStringDataWorker,
            'Data': TStringDataWorker,
            'c_str': TStringCStrWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            class_type.template_argument(2)
            # Next to adding TCowstring TBasicString gets back 3 template argument
            if str(class_type.template_argument(2)) in ['true', 'false']:
                element_type = class_type.template_argument(0)
            else:
                element_type = class_type.template_argument(1)
        except:
            # Since r5448720 TBasicString has 2 template arguments.
            try:
                element_type = class_type.template_argument(0)
            except:
                return None
        return method(class_type, element_type)


class TStringBufSizeWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.result_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.result_type

    def __call__(self, obj):
        return obj['__size_']


class TStringBufIndexWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.index_type = gdb.lookup_type('size_t')
        self.result_type = element_type.reference()

    def get_arg_types(self):
        return self.index_type

    def get_result_type(self, obj, idx):
        return self.result_type

    def __call__(self, obj, idx):
        return obj['__data_'][idx].reference_value()


class TStringBufDataWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.result_type = element_type.const().pointer()

    def get_arg_types(self):
        return None

    def get_result_type(self, obj):
        return self.result_type

    def __call__(self, obj):
        return obj['__data_'].cast(self.result_type)


class TStringBufMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'arcadia::TStringBuf')
        self.class_name = 'TBasicStringBuf'
        self.method_workers = {
            'size': TStringBufSizeWorker,
            'length': TStringBufSizeWorker,
            'Size': TStringBufSizeWorker,
            'Length': TStringBufSizeWorker,
            'operator[]': TStringBufIndexWorker,
            'data': TStringBufDataWorker,
            'Data': TStringBufDataWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            element_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, element_type)


def register_xmethods():
    gdb.xmethod.register_xmethod_matcher(None, PtrMatcher())
    gdb.xmethod.register_xmethod_matcher(None, TMaybeMatcher())
    gdb.xmethod.register_xmethod_matcher(None, TStringMatcher())
    gdb.xmethod.register_xmethod_matcher(None, TStringBufMatcher())
