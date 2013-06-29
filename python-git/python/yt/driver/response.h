#pragma once

#include "stream.h"

#include <contrib/libs/pycxx/Extensions.hxx>

#include <ytlib/driver/driver.h>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TResponse
    : public Py::PythonClass<TResponse>
{
public:
    TResponse(Py::PythonClassInstance *self, Py::Tuple &args, Py::Dict &kwds);

    void SetResponse(TFuture<NDriver::TDriverResponse> response);

    void OwnInputStream(std::unique_ptr<TPythonInputStream>& inputStream);
    
    void OwnOutputStream(std::unique_ptr<TPythonOutputStream>& outputStream);
    
    Py::Object Wait(Py::Tuple& args, Py::Dict &kwds);
    PYCXX_KEYWORDS_METHOD_DECL(TResponse, Wait);
    
    Py::Object IsSet(Py::Tuple& args, Py::Dict &kwds);
    PYCXX_KEYWORDS_METHOD_DECL(TResponse, IsSet);

    Py::Object Error(Py::Tuple& args, Py::Dict &kwds);
    PYCXX_KEYWORDS_METHOD_DECL(TResponse, Error);

    virtual ~TResponse();
    
    static void InitType();

private:
    TFuture<NDriver::TDriverResponse> Response_;

    std::unique_ptr<TPythonInputStream> InputStream_;
    std::unique_ptr<TPythonOutputStream> OutputStream_;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
