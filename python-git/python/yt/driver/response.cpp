#include "response.h"

#include "common.h"
#include "serialize.h"

#include <ytlib/ytree/convert.h>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

TResponse::TResponse(Py::PythonClassInstance *self, Py::Tuple &args, Py::Dict &kwds)
    : Py::PythonClass<TResponse>::PythonClass(self, args, kwds)
{ }

void TResponse::SetResponse(TFuture<NDriver::TDriverResponse> response)
{
    Response_ = response;
}

void TResponse::OwnInputStream(std::unique_ptr<TPythonInputStream>& inputStream)
{
    InputStream_.swap(inputStream);
}
    
void TResponse::OwnOutputStream(std::unique_ptr<TPythonOutputStream>& outputStream)
{
    OutputStream_.swap(outputStream);
}


Py::Object TResponse::Wait(Py::Tuple& args, Py::Dict &kwds)
{
    Response_.Get();
    return Py::None();
}
    
Py::Object TResponse::IsSet(Py::Tuple& args, Py::Dict &kwds)
{
    return Py::Boolean(Response_.IsSet());
}

Py::Object TResponse::IsOk(Py::Tuple& args, Py::Dict &kwds)
{
    if (!Response_.IsSet()) {
        THROW_ERROR_EXCEPTION("Response is not set");
    }
    return Py::Boolean(Response_.Get().Error.IsOK());
}

Py::Object TResponse::Error(Py::Tuple& args, Py::Dict &kwds)
{
    if (!Response_.IsSet()) {
        THROW_ERROR_EXCEPTION("Response is not set");
    }
    return NYTree::ConvertTo<Py::Object>(Response_.Get().Error);
}

TResponse::~TResponse()
{ }
    
void TResponse::InitType()
{
    behaviors().name("Response");
    behaviors().doc("Command response");
    behaviors().supportGetattro();
    behaviors().supportSetattro();

    PYCXX_ADD_KEYWORDS_METHOD(wait, Wait, "Synchronously wait command completion");
    PYCXX_ADD_KEYWORDS_METHOD(is_set, IsSet, "Check that response is finished");
    PYCXX_ADD_KEYWORDS_METHOD(is_ok, IsOk, "Check that response executed successfully (can be called only if response is set)");
    PYCXX_ADD_KEYWORDS_METHOD(error, Error, "Return error of response (can be called only if response is set)");

    behaviors().readyType();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
