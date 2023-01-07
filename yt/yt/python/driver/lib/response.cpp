#include "response.h"
#include "private.h"

#include <yt/yt/python/common/error.h>
#include <yt/yt/python/common/helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NPython {

using namespace NYson;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

std::atomic<bool> TDriverResponseHolder::ShuttingDown_;
NThreading::TSpinLock TDriverResponseHolder::DestructionSpinLock_;

NThreading::TSpinLock AliveDriverResponseHoldersLock;
THashSet<TDriverResponseHolder*> AliveDriverResponseHolders;

TDriverResponseHolder::TDriverResponseHolder()
{ }

void TDriverResponseHolder::Initialize()
{
    // TDriverResponseHolder should not be created after start of python finalization.
    // But it can happen if GCCollect initiated by finalization remove some object that releases GIL,
    // in that case other threads can try to execute some code and execute driver request.
    // YT_VERIFY(!ShuttingDown_);
    if (ShuttingDown_) {
        return;
    }

    ResponseParametersYsonWriter_ = CreateYsonWriter(
        &ResponseParametersBlobOutput_,
        EYsonFormat::Binary,
        EYsonType::MapFragment,
        /* enableRaw */ false);

    Initialized_.store(true);

    {
        auto guard = Guard(AliveDriverResponseHoldersLock);
        YT_VERIFY(AliveDriverResponseHolders.insert(this).second);
    }
}

void TDriverResponseHolder::Destroy()
{
    // Can be called multiple times, must be called under GIL.
    InputStream_.reset(nullptr);
    OutputStream_.reset(nullptr);
    ResponseParametersYsonWriter_.reset(nullptr);

    Destroyed_.store(true);
}

bool TDriverResponseHolder::IsInitialized() const
{
    return Initialized_;
}

TDriverResponseHolder::~TDriverResponseHolder()
{
    if (!Initialized_) {
        return;
    }

    if (!Destroyed_) {
        auto guard = Guard(DestructionSpinLock_);
        if (ShuttingDown_) {
            return;
        }

        {
            TGilGuard gilGuard;
            Destroy();
        }
    }

    {
        auto guard = Guard(AliveDriverResponseHoldersLock);
        YT_VERIFY(AliveDriverResponseHolders.erase(this) == 1);
    }
}

void TDriverResponseHolder::OnBeforePythonFinalize()
{
    ShuttingDown_.store(true);

    {
        auto guard = Guard(AliveDriverResponseHoldersLock);

        {
            for (auto* holder : AliveDriverResponseHolders) {
                holder->Destroy();
            }
        }
    }

    {
        TReleaseAcquireGilGuard guard;
        DestructionSpinLock_.Acquire();
    }
}

void TDriverResponseHolder::OnAfterPythonFinalize()
{
    DestructionSpinLock_.Release();
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

bool TDriverResponseHolder::IsResponseParametersReady() const
{
    return ResponseParametersYsonWriter_ && ResponseParametersFinished_;
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

TString TDriverResponse::TypeName_;

TDriverResponse::TDriverResponse(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TDriverResponse>::PythonClass(self, args, kwargs)
    , Holder_(New<TDriverResponseHolder>())
{
    Holder_->Initialize();
}

void TDriverResponse::SetResponse(TFuture<void> response)
{
    ResponseFuture_ = response;
    ResponseCookie_ = RegisterFuture(ResponseFuture_);
    if (ResponseCookie_ == InvalidFutureCookie) {
        throw CreateYtError("Finalization started");
    }
}

void TDriverResponse::SetTraceContextFinishGuard(TTraceContextFinishGuard&& guard)
{
    TraceContextFinishGuard_ = std::move(guard);
}

TIntrusivePtr<TDriverResponseHolder> TDriverResponse::GetHolder() const
{
    return Holder_;
}

Py::Object TDriverResponse::ResponseParameters(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    if (Holder_->IsResponseParametersReady()) {
        return NYTree::ConvertTo<Py::Object>(Holder_->GetResponseParametersYsonString());
    } else {
        return Py::None();
    }
}

Py::Object TDriverResponse::Wait(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    {
        TReleaseAcquireGilGuard guard;
        auto result = WaitForSettingFuture(ResponseFuture_);
        if (!result) {
            ResponseFuture_.Cancel(TError(NYT::EErrorCode::Canceled, "Wait canceled"));
        }
        UnregisterFuture(ResponseCookie_);
    }

    if (PyErr_Occurred()) {
        throw Py::Exception();
    }
    return Py::None();
}

Py::Object TDriverResponse::IsSet(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    return Py::Boolean(ResponseFuture_.IsSet());
}

Py::Object TDriverResponse::IsOk(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    if (!ResponseFuture_.IsSet()) {
        throw CreateYtError("Response is not set");
    }
    return Py::Boolean(ResponseFuture_.Get().IsOK());
}

Py::Object TDriverResponse::Error(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
{
    if (!ResponseFuture_.IsSet()) {
        throw CreateYtError("Response is not set");
    }
    Py::Object object;
#if PY_MAJOR_VERSION < 3
    Deserialize(object, NYTree::ConvertToNode(ResponseFuture_.Get()), std::nullopt);
#else
    Deserialize(object, NYTree::ConvertToNode(ResponseFuture_.Get()), std::make_optional<TString>("utf-8"));
#endif
    return object;
}

TDriverResponse::~TDriverResponse()
{
    try {
        if (ResponseFuture_) {
            ResponseFuture_.Cancel(TError(NYT::EErrorCode::Canceled, "Driver response destroyed"));
        }
    } catch (...) {
        // intentionally doing nothing
    }

    // Holder destructor must not be called from python context.
    GetFinalizerInvoker()->Invoke(BIND([holder = Holder_.Release()] { Unref(holder); }));
}

void TDriverResponse::InitType(const TString& moduleName)
{
    static std::once_flag flag;
    std::call_once(flag, [&] {
        TypeName_ = moduleName + ".Response";
        behaviors().name(TypeName_.c_str());
        behaviors().doc("Command response");
        behaviors().supportGetattro();
        behaviors().supportSetattro();

        PYCXX_ADD_KEYWORDS_METHOD(response_parameters, ResponseParameters, "Extract response parameters");
        PYCXX_ADD_KEYWORDS_METHOD(wait, Wait, "Synchronously wait command completion");
        PYCXX_ADD_KEYWORDS_METHOD(is_set, IsSet, "Check that response is finished");
        PYCXX_ADD_KEYWORDS_METHOD(is_ok, IsOk, "Check that response executed successfully (can be called only if response is set)");
        PYCXX_ADD_KEYWORDS_METHOD(error, Error, "Return error of response (can be called only if response is set)");

        behaviors().readyType();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
