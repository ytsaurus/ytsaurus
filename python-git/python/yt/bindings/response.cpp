#include "response.h"

#include "common.h"
#include "serialize.h"

#include <core/ytree/convert.h>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

TDriverResponse::TDriverResponse(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TDriverResponse>::PythonClass(self, args, kwargs)
{ }

void TDriverResponse::SetResponse(TFuture<NDriver::TDriverResponse> response)
{
    Response_ = response;
}

void TDriverResponse::OwnInputStream(std::unique_ptr<TInputStreamWrap>& inputStream)
{
    InputStream_.swap(inputStream);
}
    
void TDriverResponse::OwnOutputStream(std::unique_ptr<TOutputStreamWrap>& outputStream)
{
    OutputStream_.swap(outputStream);
}


Py::Object TDriverResponse::Wait(Py::Tuple& args, Py::Dict& kwargs)
{
    Py_BEGIN_ALLOW_THREADS
    Response_.Get();
    Py_END_ALLOW_THREADS

    return Py::None();
}
    
Py::Object TDriverResponse::IsSet(Py::Tuple& args, Py::Dict& kwargs)
{
    return Py::Boolean(Response_.IsSet());
}

Py::Object TDriverResponse::IsOk(Py::Tuple& args, Py::Dict& kwargs)
{
    if (!Response_.IsSet()) {
        THROW_ERROR_EXCEPTION("Response is not set");
    }
    return Py::Boolean(Response_.Get().Error.IsOK());
}

Py::Object TDriverResponse::Error(Py::Tuple& args, Py::Dict& kwargs)
{
    if (!Response_.IsSet()) {
        THROW_ERROR_EXCEPTION("Response is not set");
    }
    return NYTree::ConvertTo<Py::Object>(Response_.Get().Error);
}

TDriverResponse::~TDriverResponse()
{ }
    
void TDriverResponse::InitType()
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
