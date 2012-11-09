#pragma once

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

class TPythonInputStream
    : public TInputStream
{
public:
    explicit TPythonInputStream(const Py::Object& inputStream);
    virtual ~TPythonInputStream() throw();

    virtual size_t DoRead(void* buf, size_t len);

private:
    Py::Object InputStream_;
};

class TPythonOutputStream: public TOutputStream {
public:
    explicit TPythonOutputStream(const Py::Object& outputStream);
    virtual ~TPythonOutputStream() throw();

    virtual void DoWrite(const void* buf, size_t len);

private:
    Py::Object OutputStream_;
};

} // namespace NPython
} // namespace NYT

