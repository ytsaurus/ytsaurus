#include "program.h"

#include <ads/bsyeti/libs/ytex/logging/adapters/global/global.h>
#include <ads/bsyeti/libs/ytex/logging/proto/config.pb.h>
#include <ads/bsyeti/libs/ytex/logging/logging.h>
#include <library/cpp/sighandler/async_signals_handler.h>
#include <library/cpp/yt/logging/logger.h>
#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/signal_registry.h>

#include <library/cpp/protobuf/util/traits.h> //TODO: remove

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TProgram::TProgram(NYT::TCancelableContextPtr cancelableContext) : CancelableContext_(std::move(cancelableContext))
{
    if (!CancelableContext_) {
        CancelableContext_ = NYT::New<NYT::TCancelableContext>();
    }
    InitSignalHandlers();
}

TProgram::~TProgram()
{
    NYT::Shutdown();
}

NYT::TCancelableContextPtr TProgram::GetCancelableContext() const
{
    return CancelableContext_;
}

void TProgram::InitSignalHandlers()
{
    auto signalFunction = [this](int sig) mutable {
        Y_UNUSED(sig);
        this->GetCancelableContext()->Cancel(NYT::TError(NYT::EErrorCode::Canceled, "Stop signal."));
    };

    SetAsyncSignalFunction(SIGTERM, signalFunction);
    SetAsyncSignalFunction(SIGINT, signalFunction);

    {
        // code from NYT::ConfigureCrashHandler()
        // copy-pasted in order to avoid redundant dependencies to yt/yt/ytlib.
        NYT::TSignalRegistry::Get()->PushCallback(NYT::AllCrashSignals, NYT::CrashSignalHandler);
        NYT::TSignalRegistry::Get()->PushDefaultSignalHandler(NYT::AllCrashSignals);
    }
}

////////////////////////////////////////////////////////////////////////////////
//TODO: use from YTEx::Program
template <class T>
std::optional<T> GetOption(const NProtoBuf::Message& msg, const TString& name) {
    constexpr auto cppType = NProtoBuf::TSelectCppType<T>::Result;
    using TOptionTraits = NProtoBuf::TCppTypeTraits<cppType>;

    auto* field = msg.GetDescriptor()->FindFieldByName(name);
    if (nullptr == field) {
        return std::optional<T>();
    }
    Y_VERIFY(field->cpp_type() == cppType, "option type mismatch");
    return std::optional<T>(TOptionTraits::Get(msg, field));
}

static ::NYT::NLogging::TLogger Logger("Main");

void ConfigureLogs(const google::protobuf::Message& config) {
    auto logConfig = GetOption<const google::protobuf::Message*>(config, "Logs");
    Y_VERIFY(logConfig.has_value(), "missing Logs in config");
    Y_VERIFY(NYTEx::NLogging::TConfig::descriptor() == (*logConfig)->GetDescriptor(), "expected Logs has type NYTEx::NLogging::TConfig");
    const auto& logConfigTyped = static_cast<const NYTEx::NLogging::TConfig&>(**logConfig);
    NYTEx::NLogging::Initialize(logConfigTyped);
    //ExtraLogsWorkaround = logConfigTyped.GetExtraLogsWorkaround();
    auto SkipFilenameInGloblaLog = logConfigTyped.GetSkipFilenameInGlobalLog();
    NYTEx::NLogging::ConfigureGlobalLogAdapter(Logger, SkipFilenameInGloblaLog);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
