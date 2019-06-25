#include "response.h"

#include <yt/python/common/helpers.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NPython {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDriverResponseHolder::TDriverResponseHolder()
    : ResponseParametersYsonWriter_(CreateYsonWriter(
        &ResponseParametersBlobOutput_,
        EYsonFormat::Binary,
        EYsonType::MapFragment,
        /* enableRaw */ false,
        /* booleanAsString */ false))
{ }

TDriverResponseHolder::~TDriverResponseHolder()
{
    // TODO(ignat): fix possible race here.
    if (!Py_IsInitialized()) {
        return;
    }

    TGilGuard guard;
    // Releasing Python objects under GIL.
    InputStream_.reset(nullptr);
    OutputStream_.reset(nullptr);
    ResponseParametersYsonWriter_.reset(nullptr);
}

IFlushableYsonConsumer* TDriverResponseHolder::GetResponseParametersConsumer() const
{
    return ResponseParametersYsonWriter_.get();
}

TYsonString TDriverResponseHolder::GetResponseParametersYsonString() const
{
    return TYsonString(
        TString(ResponseParametersBlobOutput_.Blob().ToStringBuf()),
        EYsonType::MapFragment);
}

void TDriverResponseHolder::OnResponseParametersFinished()
{
    ResponseParametersYsonWriter_->Flush();
    ResponseParametersFinished_.store(true);
}

bool TDriverResponseHolder::IsResponseParametersFinished() const
{
    return ResponseParametersFinished_;
}

void TDriverResponseHolder::HoldInputStream(std::unique_ptr<IInputStream> inputStream)
{
    InputStream_.swap(inputStream);
}

void TDriverResponseHolder::HoldOutputStream(std::unique_ptr<IOutputStream>& outputStream)
{
    OutputStream_.swap(outputStream);
}

////////////////////////////////////////////////////////////////////////////////

TDriverResponse::TDriverResponse(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TDriverResponse>::PythonClass(self, args, kwargs)
    , Holder_(New<TDriverResponseHolder>())
{ }

void TDriverResponse::SetResponse(TFuture<void> response)
{
    Response_ = response;
}

TIntrusivePtr<TDriverResponseHolder> TDriverResponse::GetHolder() const
{
    return Holder_;
}

Py::Object TDriverResponse::ResponseParameters(Py::Tuple& args, Py::Dict& kwargs)
{
    if (Holder_->IsResponseParametersFinished()) {
        Holder_->GetResponseParametersConsumer()->Flush();
        return NYTree::ConvertTo<Py::Object>(Holder_->GetResponseParametersYsonString());
    } else {
        return Py::None();
    }
}

Py::Object TDriverResponse::Wait(Py::Tuple& args, Py::Dict& kwargs)
{
    {
        TReleaseAcquireGilGuard guard;
        auto result = WaitForSettingFuture(Response_);
        if (!result) {
            Response_.Cancel();
        }
    }

    if (PyErr_Occurred()) {
        throw Py::Exception();
    }
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
    return Py::Boolean(Response_.Get().IsOK());
}

Py::Object TDriverResponse::Error(Py::Tuple& args, Py::Dict& kwargs)
{
    if (!Response_.IsSet()) {
        THROW_ERROR_EXCEPTION("Response is not set");
    }
    Py::Object object;
#if PY_MAJOR_VERSION < 3
    Deserialize(object, NYTree::ConvertToNode(Response_.Get()), std::nullopt);
#else
    Deserialize(object, NYTree::ConvertToNode(Response_.Get()), std::make_optional<TString>("utf-8"));
#endif
    return object;
}

TDriverResponse::~TDriverResponse()
{
    try {
        if (Response_) {
            Response_.Cancel();
        }
    } catch (...) {
        // intentionally doing nothing
    }
}

void TDriverResponse::InitType()
{
    static bool Initialized_ = false;
    if (Initialized_) {
        return;
    }

    behaviors().name("Response");
    behaviors().doc("Command response");
    behaviors().supportGetattro();
    behaviors().supportSetattro();

    PYCXX_ADD_KEYWORDS_METHOD(response_parameters, ResponseParameters, "Extract response parameters");
    PYCXX_ADD_KEYWORDS_METHOD(wait, Wait, "Synchronously wait command completion");
    PYCXX_ADD_KEYWORDS_METHOD(is_set, IsSet, "Check that response is finished");
    PYCXX_ADD_KEYWORDS_METHOD(is_ok, IsOk, "Check that response executed successfully (can be called only if response is set)");
    PYCXX_ADD_KEYWORDS_METHOD(error, Error, "Return error of response (can be called only if response is set)");

    behaviors().readyType();

    Initialized_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
