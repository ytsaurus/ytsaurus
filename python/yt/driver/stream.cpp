#include "stream.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <contrib/libs/pycxx/Objects.hxx>

#include <string>
#include <iostream>

namespace NYT {
namespace NPython {

TPythonInputStream::TPythonInputStream(const Py::Object& inputStream)
    : InputStream_(inputStream)
{ }
    
TPythonInputStream::~TPythonInputStream() throw()
{ }

size_t TPythonInputStream::DoRead(void* buf, size_t len)
{
    auto args = Py::TupleN(Py::Int(static_cast<long>(len)));
    Py::Object result = InputStream_.callMemberFunction("read", args);
    if (!result.isString()) {
        throw Py::RuntimeError("Read returns non-string object");
    }
    auto data = PyString_AsString(*result);
    auto length = PyString_Size(*result);
    std::copy(data, data + length, (char*)buf);
    return length;
}


TPythonOutputStream::TPythonOutputStream(const Py::Object& outputStream)
    : OutputStream_(outputStream)
{ }

TPythonOutputStream::~TPythonOutputStream() throw()
{ }

void TPythonOutputStream::DoWrite(const void* buf, size_t len) {
    //std::string str((const char*)buf, len);
    OutputStream_.callMemberFunction("write", Py::TupleN(Py::String((const char*)buf, len)));
}

} // namespace NPython
} // namespace NYT
