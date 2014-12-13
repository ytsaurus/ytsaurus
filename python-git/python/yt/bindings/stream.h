#pragma once

#include <util/stream/str.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TInputStreamWrap
    : public TInputStream
{
public:
    explicit TInputStreamWrap(const Py::Object& inputStream);
    virtual ~TInputStreamWrap() throw();

    virtual size_t DoRead(void* buf, size_t len);

private:
    Py::Object InputStream_;
};

class TOutputStreamWrap: public TOutputStream {
public:
    explicit TOutputStreamWrap(const Py::Object& outputStream);
    virtual ~TOutputStreamWrap() throw();

    virtual void DoWrite(const void* buf, size_t len);

private:
    Py::Object OutputStream_;
    Py::Callable WriteFunction_;
};

class TOwningStringInput
    : public TInputStream
{
public:
    explicit TOwningStringInput(const Stroka& string)
        : String_(string)
        , Stream_(String_)
    { }

private:
    virtual size_t DoRead(void* buf, size_t len) override
    {
        return Stream_.Read(buf, len);
    }

    Stroka String_;
    TStringInput Stream_;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

