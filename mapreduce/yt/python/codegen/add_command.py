# -*- coding: utf-8 -*-
import re

from collections import namedtuple
from os.path import join

File = namedtuple("File", ["path", "lines"])


def read_file(path):
    with open(path, "r") as f:
        return File(path=path, lines=list(f.readlines()))


def write_file(file):
    with open(file.path, "w") as f:
        for l in file.lines:
            f.write(l)
            if not l.endswith('\n'):
                f.write('\n')


def camel_to_snake(s):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def with_indent(line_or_lines, indent):
    def impl(s):
        if s.strip() == "":
            # Don't indent empty lines.
            return s
        return indent * "    " + s
    if isinstance(line_or_lines, str):
        return impl(line_or_lines)
    return list(map(impl, line_or_lines))


def get_indent(line):
    spaces = len(line) - len(line.lstrip())
    if spaces % 4 != 0:
        raise Exception("Indent is not multiple of 4: {}".format(line))
    return spaces / 4


def to_one_line(lines):
    def f(l):
        l = l.strip()
        if l.endswith(","):
            l += " "
        return l
    return "".join(map(f, lines))


def compose_signature(return_type, name, arguments, add_semicolon=False, virtual=False, override=False, final=False):
    arguments = with_indent(arguments, 1)
    arguments = [arg + "," for arg in arguments[:-1]] + [arguments[-1]]
    addendum = ")"
    if virtual == "pure":
        addendum += " = 0"
    if final:
        addendum += " final"
    if override:
        addendum += " override"
    if add_semicolon:
        addendum += ";"
    arguments[-1] += addendum
    first_line = "{} {}(".format(return_type, name)
    if virtual is not False:
        first_line = "virtual " + first_line
    return [first_line] + arguments


def insert(file, pos, lines):
    before_context = [" " + l.rstrip() for l in file.lines[max(0, pos-4):pos]]
    after_context = [" " + l.rstrip() for l in file.lines[pos:pos+4]]
    inserted = ["+" + l for l in lines]
    print("Inserting into {name}:\nvim {name} +{pos}\n{divider}\n{text}\n{divider}".format(
        name=file.path,
        pos=pos,
        divider="-" * 20,
        text="\n".join(before_context + inserted + after_context),
    ))
    return File(path=file.path, lines=file.lines[:pos] + lines + file.lines[pos:])


class NotFoundError(RuntimeError):
    pass


def find_pos(file, predicate):
    for i, line in enumerate(file.lines):
        if predicate(line):
            return i
    else:
        raise NotFoundError("Couldn't find line by predicate in file {}".format(
            file.path,
        ))


def find_method(file, name, allow_one_line_declaration=False):
    regexp = r"^\s*(virtual)?\s*(\w|:|<|>)+\s*(\w+::)?{}\(".format(name)
    if not allow_one_line_declaration:
        regexp += r"\s*$"
    regexp = re.compile(regexp)
    try:
        begin = find_pos(file, lambda line: re.match(regexp, line))
    except NotFoundError:
        raise NotFoundError("Couldn't find method/function {} in file {}".format(name, file.path))

    i = begin
    while file.lines[i].strip().endswith(',') or file.lines[i].strip().endswith('('):
        i += 1
    if file.lines[i].strip().endswith('}') or file.lines[i].strip().endswith(';'):
        return (begin, i + 1)
    while file.lines[i].strip() != '}':
        i += 1
    return (begin, i + 1)


def find_pos_after_method(file, name, allow_one_line_declaration=False):
    return find_method(file, name, allow_one_line_declaration)[1]


def get_argument_names(arguments):
    return [arg.strip().split()[-1] for arg in arguments]


def get_argument_types(arguments):
    return [" ".join(arg.strip().split()[:-1]) for arg in arguments]


def add_nyt_namespace_qualifiers(string):
    words = string.split()

    def add(w):
        if w.startswith("T") or w.startswith("I") or w.startswith("E"):
            return "NYT::" + w
        return w
    return " ".join(map(add, words))


def get_options_type(method):
    return "T{}Options".format(method)


def get_serialize_params_function(method):
    return "SerializeParamsFor{}".format(method)


def select_client_in_interface(path, after_method):
    for base_path in ["client", "cypress", "io", "operation"]:
        try:
            # Throws if not found.
            find_pos_after_method(read_file(join(path, "interface/{}.h".format(base_path))), after_method)
            return base_path
        except:
            pass
    else:
        raise Exception("Cannot find client in interface")


def select_client_class(path, after_method):
    header = read_file(join(path, "client/client.h"))
    pos = find_pos_after_method(header, after_method)
    while pos >= 0:
        if "class" in header.lines[pos]:
            return header.lines[pos].split()[-1]
        pos -= 1
    else:
        raise Exception("Ill formed code?")


def get_placeholder():
    return "%%%--WRITE-CODE-HERE--%%%"


def print_alert(msg):
    width = 60
    exclamation = "!" * width
    print("\n\n    {exc}\n    !!! {msg:^{len}}!!!\n    {exc}\n\n".format(
        exc=exclamation,
        msg=msg,
        len=width - 7,
    ))


class Generator(object):
    def __init__(
        self,
        command, method, return_type, positional_arguments,
        after_method, transactional, mutating, http_method,
        dry_run, mapreduce_yt_path,
        **kwargs
    ):
        self.command = command
        self.method = method
        self.return_type = return_type
        self.positional_arguments = positional_arguments
        self.after_method = after_method
        self.transactional = transactional
        self.mutating = mutating
        self.http_method = http_method
        self.path = mapreduce_yt_path
        self.dry_run = dry_run
        self.warnings = []

    def get_client_arguments(self, default_for_options, types_only=False):
        arguments = []
        arguments += self.positional_arguments
        options_type = get_options_type(self.method)
        arguments.append("const {}& options".format(options_type))
        if types_only:
            arguments = get_argument_types(arguments)
        if default_for_options:
            arguments[-1] += " = {}()".format(options_type)
        return arguments

    def get_serialize_params_call_args(self):
        res = []
        if self.transactional:
            res.append("transactionId")
        res += get_argument_names(self.positional_arguments)
        res.append("options")
        return ", ".join(res)

    def try_insert_after(self, file, after_method, code):
        try:
            begin, end = find_method(file, after_method)
            indent = get_indent(file.lines[begin])
            file = insert(file, end, with_indent(code, indent))
            if not self.dry_run:
                write_file(file)
        except NotFoundError:
            msg = "Failed to find method {} to insert the code after in file {}".format(
                after_method,
                file.path,
            )
            self.warnings.append("{} . Please insert the code by hand:\n{}".format(msg, "\n".join(code)))

    def get_batch_request_parser_type(self, return_type):
        if return_type == "void":
            return "TVoidResponseParser"
        elif return_type.startswith("T") and return_type.endswith("Id"):
            return "TGuidResponseParser"
        else:
            self.warnings.append("Failed to insert correct response parser type for {}, grep by {}".format(
                return_type,
                get_placeholder(),
            ))
            return get_placeholder()

    def get_response_parsing_expr(self, return_type):
        if return_type.startswith("T") and return_type.endswith("Id"):
            return "ParseGuidFromResponse(response.Response)"
        else:
            self.warnings.append("Failed to insert correct response parsing code for {}, grep by {}".format(
                return_type,
                get_placeholder(),
            ))
            return get_placeholder()

    def report_warnings(self):
        if not self.warnings:
            return
        print_alert("WARNINGS")
        for msg in self.warnings:
            print(msg)
            print("-"*50)

    def add_function(self, base_path, after_method, return_type, name, arguments,
                     code=None, dry_run=True, class_name=None, virtual=False, override=False,
                     header_return_type=None,
                     insert_empty_line_before=True):
        header = read_file(base_path + ".h")
        header_signature = compose_signature(
            return_type if header_return_type is None else header_return_type,
            name,
            arguments,
            add_semicolon=True,
            virtual=virtual,
            override=override,
        )
        if insert_empty_line_before:
            header_signature.insert(0, "")
        self.try_insert_after(header, after_method, header_signature)

        if code is None:
            return
        cpp = read_file(base_path + ".cpp")
        name_with_class = name
        if class_name is not None:
            name_with_class = "{}::{}".format(class_name, name)

        def remove_default(argument):
            idx = argument.find('=')
            if idx != -1:
                return argument[:idx].strip()
            return argument
        cpp_arguments = list(map(remove_default, arguments))

        cpp_signature = compose_signature(return_type, name_with_class, cpp_arguments)
        cpp_code = cpp_signature + ["{"] + with_indent(code, 1) + ["}"]
        if insert_empty_line_before:
            cpp_code.insert(0, "")
        self.try_insert_after(cpp, after_method, cpp_code)

    def add_serialize_params(self):
        arguments = []
        if self.transactional:
            arguments.append("const TTransactionId& transactionId")
        arguments += self.get_client_arguments(default_for_options=False)

        code = ["TNode result;"]
        if self.transactional:
            code.append("SetTransactionIdParam(&result, transactionId);")
        for type, name in zip(get_argument_types(arguments), get_argument_names(arguments)):
            if name == "options":
                continue
            if type == "const TYPath&" and name == "path":
                code.append("SetPathParam(&result, path);")
            code.append('result["{}"] = {};'.format(camel_to_snake(name), name))
        code.append("Y_UNUSED(options);")
        code.append("return result;")

        base_path = join(self.path, "raw_client/rpc_parameters_serialization")
        self.warnings.append("Added possibly wrong code for parameter serialization in file {} "
                             ", please check it".format(base_path + ".cpp"))

        self.add_function(
            base_path=base_path,
            after_method=get_serialize_params_function(self.after_method),
            return_type="TNode",
            name=get_serialize_params_function(self.method),
            arguments=arguments,
            code=code,
            dry_run=self.dry_run,
        )

    def add_options(self):
        header = read_file(join(self.path, "interface/client_method_options.h"))
        after_options_type = get_options_type(self.after_method)
        pos = find_pos(header, lambda l: l.strip() == "struct {}".format(after_options_type))
        while header.lines[pos].strip() != "};":
            pos += 1

        options_type = get_options_type(self.method)
        to_insert = [
            "",
            "// https://wiki.yandex-team.ru/yt/userdoc/api/#{}".format(self.command),
            "struct {}".format(options_type),
            "{",
            "    using TSelf = {};".format(options_type),
            "};",
        ]
        header = insert(header, pos + 1, to_insert)
        if not self.dry_run:
            write_file(header)

        fwd = read_file(join(self.path, "interface/fwd.h"))
        pos = find_pos(fwd, lambda l: l.strip() == "struct {};".format(after_options_type))
        to_insert = [
            "",
            "struct {};".format(options_type),
        ]
        fwd = insert(fwd, pos + 1, with_indent(to_insert, 1))
        if not self.dry_run:
            write_file(fwd)

    def add_raw_method(self):
        arguments = ["const TAuth& auth"]
        if self.transactional:
            arguments.append("const TTransactionId& transactionId")
        arguments += self.get_client_arguments(default_for_options=True)
        arguments.append("IRequestRetryPolicyPtr retryPolicy = nullptr")

        code = [
            'THttpHeader header("{http_method}", "{name}");'.format(http_method=self.http_method, name=self.command),
        ]
        if self.mutating:
            code.append("header.AddMutationId();")
        code.append(
            "header.MergeParameters(SerializeParamsFor{name}({serialize_params}));".format(
                name=self.method,
                serialize_params=self.get_serialize_params_call_args(),
            ))
        if self.return_type == "void":
            code.append('RetryRequestWithPolicy(auth, header, "", retryPolicy);')
        else:
            code.append('auto response = RetryRequestWithPolicy(auth, header, "", retryPolicy);')
            code.append("return " + self.get_response_parsing_expr(self.return_type) + ";")

        self.add_function(
            base_path=join(self.path, "raw_client/raw_requests"),
            after_method=self.after_method,
            return_type=self.return_type,
            name=self.method,
            arguments=arguments,
            code=code,
            dry_run=self.dry_run,
        )

    def add_client_method(self):
        self.add_function(
            base_path=join(self.path, "interface/{}".format(select_client_in_interface(self.path, self.after_method))),
            after_method=self.after_method,
            return_type=self.return_type,
            name=self.method,
            arguments=self.get_client_arguments(default_for_options=True),
            virtual="pure",
            dry_run=self.dry_run,
        )

        raw_call_args = ["Auth_"]
        if self.transactional:
            raw_call_args.append("TransactionId_")
        raw_call_args += get_argument_names(self.positional_arguments)
        raw_call_args.append("options")
        raw_call = "NRawClient::{}({});".format(self.method, ", ".join(raw_call_args))
        if self.return_type == "void":
            code = [raw_call]
        else:
            code = ["return " + raw_call]

        self.add_function(
            base_path=join(self.path, "client/client"),
            after_method=self.after_method,
            return_type=self.return_type,
            name=self.method,
            arguments=self.get_client_arguments(default_for_options=False),
            override=True,
            class_name=select_client_class(self.path, self.after_method),
            code=code,
            dry_run=self.dry_run,
        )

    def add_mock(self):
        header = read_file(join(self.path, "library/mock_client/yt_mock.h"))
        pos = find_pos(
            header,
            lambda line: line.strip().startswith("MOCK_METHOD") and self.after_method in line,
        )

        arguments = get_argument_types(self.get_client_arguments(default_for_options=False))
        to_insert = "MOCK_METHOD{arg_count}({name}, {return_type}({arguments}));".format(
            arg_count=len(arguments),
            name=self.method,
            return_type=self.return_type,
            arguments=", ".join(arguments),
        )

        header = insert(header, pos + 1, with_indent([to_insert], 2))
        if not self.dry_run:
            write_file(header)

    def add_raw_batch_method(self):
        arguments = []
        if self.transactional:
            arguments.append("const TTransactionId& transactionId")
        arguments += self.positional_arguments
        options_type = get_options_type(self.method)
        arguments.append("const {}& options".format(options_type))

        parser_type = self.get_batch_request_parser_type(self.return_type)
        code = [
            'return AddRequest<{}>('.format(parser_type),
            '    "{}",'.format(self.command),
            '    SerializeParamsFor{}({}),'.format(
                self.method,
                self.get_serialize_params_call_args(),
            ),
            '    Nothing());',
        ]

        return_type = "TFuture<{}>".format(self.return_type)
        self.add_function(
            base_path=join(self.path, "raw_client/raw_batch_request"),
            after_method=self.after_method,
            return_type=return_type,
            header_return_type="NThreading::" + return_type,
            name=self.method,
            arguments=arguments,
            code=code,
            class_name="TRawBatchRequest",
            dry_run=self.dry_run,
            insert_empty_line_before=False,
        )

    def add_client_batch_method(self):
        return_type = "TFuture<{}>".format(self.return_type)
        self.add_function(
            base_path=join(self.path, "interface/batch_request"),
            after_method=self.after_method,
            return_type=return_type,
            header_return_type="NThreading::" + return_type,
            name=self.method,
            arguments=self.get_client_arguments(default_for_options=True),
            virtual="pure",
            dry_run=self.dry_run,
        )

        impl_call_arguments = []
        if self.transactional:
            impl_call_arguments.append("DefaultTransaction_")
        impl_call_arguments += get_argument_names(self.positional_arguments)
        impl_call_arguments.append("options")
        code = [
            "return Impl_->{}({});".format(self.method, ", ".join(impl_call_arguments)),
        ]

        self.add_function(
            base_path=join(self.path, "client/batch_request_impl"),
            after_method=self.after_method,
            return_type=return_type,
            header_return_type="NThreading::" + return_type,
            name=self.method,
            arguments=self.get_client_arguments(default_for_options=False),
            code=code,
            override=True,
            class_name="TBatchRequest",
            dry_run=self.dry_run,
        )

    def add_extsearch_mock(self):
        if not self.transactional:
            print_alert("Skipping extsearch mock {} because it implements only IClientBase".format(
                "../../extsearch/images/robot/library/yt_test/client.h",
            ))
            return

        mock_class_name = "{}Mock".format(select_client_class(self.path, self.after_method))

        def get_signature(in_header):
            arguments = self.get_client_arguments(default_for_options=in_header)
            if in_header:
                name = self.method
            else:
                name = "{}::{}".format(mock_class_name, self.method)
            return to_one_line(compose_signature(
                return_type=add_nyt_namespace_qualifiers(self.return_type),
                name=name,
                arguments=list(map(add_nyt_namespace_qualifiers, arguments)),
                virtual=in_header,
                final=in_header,
                add_semicolon=in_header,
            ))

        header = read_file(join(self.path, "../../extsearch/images/robot/library/yt_test/client.h"))
        insertion_pos = find_pos_after_method(header, self.after_method, allow_one_line_declaration=True)
        header_code = with_indent([get_signature(True)], 1)
        header = insert(header, insertion_pos, header_code)
        if not self.dry_run:
            write_file(header)

        cpp = read_file(join(self.path, "../../extsearch/images/robot/library/yt_test/client.cpp"))
        insertion_pos = find_pos_after_method(cpp, self.after_method)
        cpp_code = ["", get_signature(False) + " {"]
        cpp_code.append(with_indent(
            'Y_ENSURE(false, "{}Mock::{} is unimplemented!);"'.format(mock_class_name, self.method),
            1,
        ))
        cpp_code.append("}")
        cpp = insert(cpp, insertion_pos, cpp_code)
        if not self.dry_run:
            write_file(cpp)

    def add_market_mock(self):
        header = read_file(join(self.path, "../../market/idx/feeds/qparser/tests/mock_yt_client.h"))
        code = compose_signature(
            self.return_type,
            self.method,
            self.get_client_arguments(default_for_options=True, types_only=True),
            virtual=True,
            override=True,
        )
        code.insert(0, "")
        code[-1] += ' { ythrow yexception() << "Not Implemented"; }'
        self.try_insert_after(header, self.after_method, code)


def add_command(**kwargs):
    generator = Generator(**kwargs)

    generator.add_serialize_params()
    generator.add_raw_method()
    generator.add_client_method()
    generator.add_options()
    generator.add_mock()

    if kwargs["add_batch_method"]:
        generator.add_raw_batch_method()
        generator.add_client_batch_method()

    if kwargs["add_external_mocks"]:
        generator.add_market_mock()
        generator.add_extsearch_mock()

    generator.report_warnings()
