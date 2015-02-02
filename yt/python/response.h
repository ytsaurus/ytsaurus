#pragma once

#include "stream.h"

#include <contrib/libs/pycxx/Extensions.hxx>

#include <ytlib/driver/driver.h>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TDriverResponse
    : public Py::PythonClass<TDriverResponse>
{
public:
    TDriverResponse(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs);

    void SetResponse(TFuture<void> response);

    void OwnInputStream(std::unique_ptr<TInputStreamWrap>& inputStream);
    
    void OwnOutputStream(std::unique_ptr<TOutputStreamWrap>& outputStream);
    
    Py::Object Wait(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, Wait);
    
    Py::Object IsSet(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, IsSet);
    
    Py::Object IsOk(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, IsOk);

    Py::Object Error(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, Error);

    virtual ~TDriverResponse();
    
    static void InitType();

private:
    TFuture<void> Response_;

    std::unique_ptr<TInputStreamWrap> InputStream_;
    std::unique_ptr<TOutputStreamWrap> OutputStream_;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
