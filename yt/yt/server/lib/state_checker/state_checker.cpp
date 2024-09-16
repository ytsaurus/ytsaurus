#include "private.h"
#include "state_checker.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NStateChecker {

using namespace NLogging;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStateChecker::TStateChecker(IInvokerPtr invoker, NApi::IClientPtr nativeClient, TString instancePath, TDuration stateCheckPeriod)
    : Logger(StateCheckerLogger())
    , Invoker_(std::move(invoker))
    , NativeClient_(std::move(nativeClient))
    , InstancePath_(std::move(instancePath))
{
    Banned_.store(false);
    StateCheckerExecutor_ = New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TStateChecker::DoCheckState, MakeStrong(this)),
        stateCheckPeriod);
}

void TStateChecker::Start()
{
    StateCheckerExecutor_->Start();
}

void TStateChecker::SetPeriod(TDuration stateCheckPeriod)
{
    StateCheckerExecutor_->SetPeriod(stateCheckPeriod);
}

bool TStateChecker::IsComponentBanned()
{
    return Banned_.load();
}

////////////////////////////////////////////////////////////////////////////////

void TStateChecker::DoCheckState()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Checking component state started");
    auto logFinally = Finally([&] {
        YT_LOG_DEBUG("Checking component state finished (Banned: %v)", Banned_.load());
    });

    auto options = NApi::TGetNodeOptions{
        .Attributes = NYTree::TAttributeFilter({NApi::BannedAttributeName}),
    };

    try {
        auto yson = WaitFor(NativeClient_->GetNode(InstancePath_, options))
            .ValueOrThrow();
        auto instance = ConvertTo<NYTree::INodePtr>(yson);

        auto banned = instance->Attributes().Get<bool>(NApi::BannedAttributeName);
        Banned_.store(banned);

    } catch (std::exception& ex) {
        YT_LOG_ERROR(ex, "Checking component state failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NStateChecker
