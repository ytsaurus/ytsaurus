#include <ytlib/ytree/convert.h>

#include "response.h"
#include "common.h"

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

Py::Object TResponse::Error(Py::Tuple& args, Py::Dict &kwds)
{
    if (!Response_.IsSet()) {
        THROW_ERROR_EXCEPTION("Response is not set");
    }
    return ConvertToPythonString(NYTree::ConvertToYsonString(Response_.Get().Error).Data());
}

TResponse::~TResponse()
{ }
    
void TResponse::InitType()
{
    behaviors().name("Response");
    behaviors().doc("Some documentation");
    behaviors().supportGetattro();
    behaviors().supportSetattro();

    PYCXX_ADD_KEYWORDS_METHOD(wait, Wait, "TODO(ignat): make documentation");
    PYCXX_ADD_KEYWORDS_METHOD(is_set, IsSet, "TODO(ignat): make documentation");
    PYCXX_ADD_KEYWORDS_METHOD(error, Error, "TODO(ignat): make documentation");

    behaviors().readyType();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
