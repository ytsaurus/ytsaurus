#include "stream.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <contrib/libs/pycxx/Objects.hxx>

#include <string>
#include <iostream>

namespace NYT {
namespace NPython {

TPythonInputStream::TPythonInputStream(Py::Object inputStream)
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
    // TODO(ignat): it is very inoptimal implementation
    std::string resultStr = Py::String(result).as_std_string();
    std::cerr << resultStr << std::endl;
    std::copy(resultStr.begin(), resultStr.end(), (char*)buf);
    return resultStr.size();
}


TPythonOutputStream::TPythonOutputStream(Py::Object stream)
    : Stream_(stream)
{ }

TPythonOutputStream::~TPythonOutputStream() throw()
{ }

void TPythonOutputStream::DoWrite(const void* buf, size_t len) {
    std::string str((const char*)buf, len);
    Stream_.callMemberFunction("write", Py::TupleN(Py::String(str)));
}

} // namespace NPython
} // namespace NYT
