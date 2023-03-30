# -*- coding: utf-8 -*-
import signal
import subprocess
import os

import pytest
import yatest.common as yc

import re

gdbdir = os.path.dirname(os.path.dirname(yc.gdb_path()))
gdbpath = gdbdir + '/bin/bin_gdb/gdb'
gdbinit = yc.source_path('devtools/gdb/__init__.py')
gdbtest = yc.binary_path('devtools/gdb/test/gdbtest/gdbtest')

# Do not suspend with SIGTTOU when running in the background process group.
signal.signal(signal.SIGTTOU, signal.SIG_IGN)


def gdb(*commands):
    cmd = [gdbpath, '-nx', '-batch', '-ix', gdbinit, '-ex', 'set charset UTF-8']
    for c in commands:
        cmd += ['-ex', c]
    cmd += [gdbtest]
    env = os.environ
    env['PYTHONHOME'] = gdbdir
    env['PYTHONPATH'] = gdbdir + '/share/gdb/python'
    # strings are not printed correctly in gdb overwise.
    env['LC_ALL'] = 'en_US.UTF-8'
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
    return out


def run(function):
    """
    Pretty print function return value.
    """
    return gdb('b {}'.format(function), 'run', 'finish')


def data(field):
    """
    Pretty print global variable.
    """
    return gdb('b stop_here', 'run', 'p {}'.format(field))


tests = dict(
    test_tuple='tuple = {[0] = 1, [1] = 2}',
    test_vector='std::__y1::vector (length=4, capacity=4) = {[0] = 1, [1] = 2, [2] = 3, [3] = 4}',
    test_hashmap='THashMap of length 2 = {[0] 1 = 2, [1] 3 = 4}',
    test_hashset='THashSet of length 3 = {[0] = 1, [1] = 2, [2] = 3}',
    test_hashmultimap='THashMultiMap of length 3 = {[0] 1 = 2, [1] 1 = 4, [2] 3 = 4}',
    test_tstring_hashmap='THashMap of length 1 = {[0] Это = тест}',
    test_bitset='std::__y1::bitset (length=8) = {[0] = 1, [2] = 1, [5] = 1, [7] = 1}',
    test_map='std::__y1::map (count=5) = {[0] 1 = 2, [1] 3 = 4, [2] 5 = 6, [3] 7 = 8, [4] 9 = 10}',
    test_multimap='std::__y1::multimap (count=3) = {[0] 1 = 2, [1] 1 = 4, [2] 3 = 4}',
    test_set='std::__y1::set (count=6) = {[0] = 1, [1] = 2, [2] = 3, [3] = 4, [4] = 5, [5] = 6}',
    test_unordered_map='std::__y1::unordered_map (count=1) = {[0] 1 = 2}',
    test_unordered_set='std::__y1::unordered_set (count=1) = {[0] = 1}',
    test_deque='std::__y1::deque (size=4) = {[0] = 1, [1] = 2, [2] = 3, [3] = 4}',
    test_pair='pair = {first = 1, second = 2}',
    test_array='{__elems_ = {1, 2, 3, 4}}',
    test_string='"Это тест."',
    test_string_view='"Это тест."',
    test_unique_int=re.compile(r'= std::__y1::unique_ptr<int> = 0x[0-9a-f]+'),
    test_unique_empty='std::__y1::unique_ptr<int> = 0x0',
    test_shared_int=re.compile(r'(= )?std::__y1::shared_ptr \(count 1, weak 0\) = 0x[0-9a-f]+'),
    test_shared_empty='std::__y1::shared_ptr (empty) = 0x0',
    test_tvector='TVector (length=4, capacity=4) = {[0] = 1, [1] = 2, [2] = 3, [3] = 4}',
    test_tstring='Это тест.',
    test_tutf16string='L"Это тест."',
    test_tstringbuf='"Это тест."',
    test_tvariant_default='TVariantType = {int = 0}',
    test_tvariant_int='TVariantType = {int = 10}',
    test_tvariant_string='TVariantType = {TString = Это тест.}',
    test_maybe='TMaybe = {GetRef() = 465}',
    test_maybe_empty='TMaybe empty',
    test_enumbitset='TEnumBitSet = {A, B}',
    test_holder_int=re.compile(r'THolder<int> = {Get\(\) = 0x[0-9a-f]+ <test_int>}'),
    test_holder_empty='THolder<int> = {Get() = 0x0}',
    test_holder_string_empty='THolder<std::string> = {Get() = 0x0}',
    test_cow_shared_empty='TCowPtr = TSharedPtr<int> = {Get() = 0x0}',
    test_cow_shared=re.compile(r'TCowPtr = TSharedPtr<int> = {Get\(\) = 0x[0-9a-f]+}'),
    test_map_iterator='[1] 2',
    test_multimap_iterator='[1] 2',
    test_set_iterator='1',
    test_deque_iterator='1',
    test_unordered_map_iterator='[1] 2',
    test_unordered_set_iterator='1',
    test_optional_int='std::optional = {value() = 123}',
    test_optional_int_empty='std::optional empty',
    test_atomic_int='std::atomic = {load() = 12}',
    test_atomic_array='std::atomic = {load() = {__elems_ = {1, 2, 3}}}',
    test_list_empty='empty std::__y1::list',
    test_list='std::__y1::list = {[0] = 1, [1] = 2, [2] = 3}'
)


@pytest.mark.parametrize(('test_name', 'test_output'), tests.items(), ids=tests.keys())
def test_pretty_printers(test_name, test_output):
    actual_output = data(test_name)
    if test_name == 'test_shared_int' and 'no member named __shared_owners_' in actual_output:
        pytest.xfail('test_shared_int fails when libcxx is built with -gline-tables-only')
    elif hasattr(test_output, 'search'):
        assert test_output.search(actual_output) is not None, actual_output.decode('UTF-8')
    else:
        assert '$1 = {}\n'.format(test_output) in actual_output, actual_output.decode('UTF-8')


xmethod_tests = dict(
    test_holder_empty_get=('test_holder_empty.Get()', '= (int *) 0x0'),
    test_holder_int_get=('test_holder_int.Get()', re.compile(r'= \(int \*\) 0x[0-9a-f]+ <test_int>')),
    test_holder_int_star=('*test_holder_int', '= 1'),
    test_vector_size=('test_vector.size()', '= 4'),
    test_vector_elem=('test_vector[1]', re.compile(r'= \(int &\) @0x[0-9a-f]+: 2')),
    test_vector_data=('test_vector.data()', re.compile(r'= \(int \*\) 0x[0-9a-f]+')),
    test_tvector_size=('test_tvector.size()', '= 4'),
    test_tvector_elem=('test_tvector[1]', re.compile(r'= \(int &\) @0x[0-9a-f]+: 2')),
    test_tmaybe_getref=('test_maybe.GetRef()', re.compile(r' = \(int &\) @0x[0-9a-f]+: 465')),
    test_tmaybe_get=('test_maybe.Get()', re.compile(r'= \(int \*\) 0x[0-9a-f]+ <test_maybe>')),
    test_tmaybe_empty_get=('test_maybe_empty.Get()', '= (void *) 0x0'),
    test_array_size=('test_array.size()', '= 4'),
    test_array_elem=('test_array[2]', re.compile(r'= \(int &\) @0x[0-9a-f]+: 3')),
    test_array_data=('test_array.data()', re.compile(r'= \(int \*\) 0x[0-9a-f]+ <test_array>')),
    test_string_length=('test_string.length()', '= 16'),
    test_string_elem=('test_string[15]', re.compile(r'= \(char &\) @0x[0-9a-f]+: 46 \'.\'')),
    test_string_data=('test_string.data()', re.compile(r'= 0x[0-9a-f]+ <test_string\+[0-9a-z]> "Это тест\."')),
    test_string_c_str=('test_string.c_str()', re.compile(r'= 0x[0-9a-f]+ <test_string\+[0-9a-z]> "Это тест\."')),
    test_tstring_length=('test_tstring.length()', '= 16'),
    test_tstring_elem=('test_tstring[15]', re.compile(r'= \(char &\) @0x[0-9a-f]+: 46 \'.\'')),
    test_tstring_data=('test_tstring.data()', re.compile(r'= 0x[0-9a-f]+ "Это тест\."')),
    test_tstring_c_str=('test_tstring.c_str()', re.compile(r'= 0x[0-9a-f]+ "Это тест\."')),
    test_tstringbuf_length=('test_tstringbuf.length()', '= 16'),
    test_tstringbuf_elem=('test_tstringbuf[15]', re.compile(r'= \(const char &\) @0x[0-9a-f]+: 46 \'.\'')),
    test_tstringbuf_data=('test_tstringbuf.data()', re.compile(r'= 0x[0-9a-f]+ "Это тест\."')),
    test_unique_ptr_get=('test_unique_int.get()', re.compile(r'= \(int \*\) 0x[0-9a-f]+')),
    test_unique_ptr_get_value=('*test_unique_int.get()', '= 1'),
    test_unique_ptr_star=('*test_unique_int', re.compile(r'= \(int &\) @0x[0-9a-f]+: 1')),
    test_shared_ptr_get=('test_shared_int.get()', re.compile(r'= \(int \*\) 0x[0-9a-f]+')),
    test_shared_ptr_get_value=('*test_shared_int.get()', '= 1'),
    test_shared_ptr_star=('*test_shared_int', re.compile(r'= \(int &\) @0x[0-9a-f]+: 1')),
    test_deque_size=('test_deque.size()', '= 4'),
    test_deque_elem=('test_deque[1]', re.compile(r'= \(int &\) @0x[0-9a-f]+: 2')),
    test_optional_value=('test_optional_int.value()', re.compile(r' = \(int &\) @0x[0-9a-f]+: 123')),
    test_optional_star=('*test_optional_int', re.compile(r' = \(int &\) @0x[0-9a-f]+: 123')),
    test_optional_has_value=('test_optional_int.has_value()', '= true'),
    test_optional_has_value_not=('test_optional_int_empty.has_value()', '= false'),
    test_atomic_load=('test_atomic_int.load()', '= 12')
)


@pytest.mark.parametrize(('test_name', 'test_check'), xmethod_tests.items(), ids=xmethod_tests.keys())
def test_xmethod(test_name, test_check):
    test_call, test_output = test_check
    actual_output = data(test_call)
    if hasattr(test_output, 'search'):
        assert test_output.search(actual_output) is not None, test_call + '\n' + actual_output
    else:
        assert test_output in actual_output, test_call + '\n' + actual_output
