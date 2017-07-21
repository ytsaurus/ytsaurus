"""Extract, format and print information about Python stack traces."""

import linecache
import sys
import types

def _format_final_exc_line(etype, value):
    """Return a list of a single line -- normal case for format_exception_only"""
    valuestr = _some_str(value)
    if value is None or not valuestr:
        line = "%s\n" % etype
    else:
        line = "%s: %s\n" % (etype, valuestr)
    return line

def _some_str(value):
    try:
        return str(value)
    except Exception:
        pass
    try:
        value = unicode(value)
        return value.encode("ascii", "backslashreplace")
    except Exception:
        pass
    return '<unprintable %s object>' % type(value).__name__

def format_exception_only(etype, value):
    """Format the exception part of a traceback.

    The arguments are the exception type and value such as given by
    sys.last_type and sys.last_value. The return value is a list of
    strings, each ending in a newline.

    Normally, the list contains a single string; however, for
    SyntaxError exceptions, it contains several lines that (when
    printed) display detailed information about where the syntax
    error occurred.

    The message indicating which exception occurred is always the last
    string in the list.

    """

    # An instance should not have a meaningful value parameter, but
    # sometimes does, particularly for string exceptions, such as
    # >>> raise string1, string2  # deprecated
    #
    # Clear these out first because issubtype(string1, SyntaxError)
    # would throw another exception and mask the original problem.
    if (isinstance(etype, BaseException) or
        isinstance(etype, types.InstanceType) or
        etype is None or type(etype) is str):
        return [_format_final_exc_line(etype, value)]

    stype = etype.__name__

    if not issubclass(etype, SyntaxError):
        return [_format_final_exc_line(stype, value)]

    # It was a syntax error; show exactly where the problem was found.
    lines = []
    try:
        msg, (filename, lineno, offset, badline) = value.args
    except Exception:
        pass
    else:
        filename = filename or "<string>"
        lines.append('  File "%s", line %d\n' % (filename, lineno))
        if badline is not None:
            lines.append('    %s\n' % badline.strip())
            if offset is not None:
                caretspace = badline.rstrip('\n')[:offset].lstrip()
                # non-space whitespace (likes tabs) must be kept for alignment
                caretspace = ((c.isspace() and c or ' ') for c in caretspace)
                # only three spaces to account for offset1 == pos 0
                lines.append('   %s^\n' % ''.join(caretspace))
        value = msg

    lines.append(_format_final_exc_line(stype, value))
    return lines

def format_exception(etype, value, tb, limit = None):
    """Format a stack trace and the exception information.

    The arguments have the same meaning as the corresponding arguments
    to print_exception().  The return value is a list of strings, each
    ending in a newline and some containing internal newlines.  When
    these lines are concatenated and printed, exactly the same text is
    printed as does print_exception().
    """
    if tb:
        list = ['Traceback (most recent call last):\n']
        list = list + format_tb(tb, limit)
    else:
        list = []
    list = list + format_exception_only(etype, value)
    return list

def format_exc(limit=None):
    """Like print_exc() but return a string."""
    try:
        etype, value, tb = sys.exc_info()
        return ''.join(format_exception(etype, value, tb, limit))
    finally:
        etype = value = tb = None

def format_list(extracted_list):
    """Format a list of traceback entry tuples for printing.

    Given a list of tuples as returned by extract_tb() or
    extract_stack(), return a list of strings ready for printing.
    Each string in the resulting list corresponds to the item with the
    same index in the argument list.  Each string ends in a newline;
    the strings may contain internal newlines as well, for those items
    whose source text line is not None.
    """
    list = []
    for filename, lineno, name, line, locals in extracted_list:
        item = '  File "%s", line %d, in %s\n' % (filename,lineno,name)
        if line:
            item = item + '    %s\n' % line.strip()
        ##### Patched
        if locals:
            item = item + '    Locals: {\n'
            for key, value in locals.items():
                item = item + '    %15s = ' % (str(key))
                try:
                    item = item + '%r}\n' % (value)
                except:
                    item = item + '<ERROR WHILE PRINTING>\n'
            item = item + '    }\n'
        #####
        list.append(item)
    return list

def format_tb(tb, limit = None):
    """A shorthand for 'format_list(extract_stack(f, limit))."""
    return format_list(extract_tb(tb, limit))

def extract_tb(tb, limit = None):
    """Return list of up to limit pre-processed entries from traceback.

    This is useful for alternate formatting of stack traces.  If
    'limit' is omitted or None, all entries are extracted.  A
    pre-processed stack trace entry is a quadruple (filename, line
    number, function name, text) representing the information that is
    usually printed for a stack trace.  The text is a string with
    leading and trailing whitespace stripped; if the source is not
    available it is None.
    """
    if limit is None:
        if hasattr(sys, 'tracebacklimit'):
            limit = sys.tracebacklimit
    list = []
    n = 0
    while tb is not None and (limit is None or n < limit):
        f = tb.tb_frame
        lineno = tb.tb_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        ##### Patched
        locals = f.f_locals
        #####
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        if line: line = line.strip()
        else: line = None
        list.append((filename, lineno, name, line, locals))
        tb = tb.tb_next
        n = n+1
    return list
