#include "stream.h"
#include "helpers.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <contrib/libs/pycxx/Objects.hxx>

#include <iostream>
#include <string>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

TInputStreamWrap::TInputStreamWrap(const Py::Object& inputStream)
    : InputStream_(inputStream)
    , ReadFunction_(InputStream_.getAttr("read"))
{ }

TInputStreamWrap::~TInputStreamWrap() throw()
{ }

size_t TInputStreamWrap::DoRead(void* buf, size_t len)
{
    TGilGuard guard;

    auto args = Py::TupleN(Py::Long(static_cast<long>(len)));
    Py::Object result = ReadFunction_.apply(args);
#if PY_MAJOR_VERSION < 3
    // COMPAT: Due to implicit promotion to unicode it is sane to work with
    // unicode objects too.
    if (!PyBytes_Check(result.ptr()) && !PyUnicode_Check(result.ptr())) {
        throw Py::RuntimeError("Read returns non-string object");
    }
#else
    if (!PyBytes_Check(result.ptr())) {
        throw Py::RuntimeError("Input stream should be binary");
    }
#endif
    auto data = PyBytes_AsString(*result);
    auto length = PyBytes_Size(*result);
    std::copy(data, data + length, (char*)buf);
    return length;
}


TOutputStreamWrap::TOutputStreamWrap(const Py::Object& outputStream)
    : OutputStream_(outputStream)
    , WriteFunction_(OutputStream_.getAttr("write"))
{ }

TOutputStreamWrap::~TOutputStreamWrap() throw()
{ }

void TOutputStreamWrap::DoWrite(const void* buf, size_t len)
{
    TGilGuard guard;
    WriteFunction_.apply(Py::TupleN(Py::Bytes(
        reinterpret_cast<const char*>(buf),
        len)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
