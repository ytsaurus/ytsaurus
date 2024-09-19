#include "private.h"
#include "state_checker.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NStateChecker {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStateChecker::TStateChecker(IInvokerPtr invoker, IClientPtr nativeClient, TYPath instancePath, TDuration stateCheckPeriod)
    : Logger(StateCheckerLogger())
    , Invoker_(std::move(invoker))
    , NativeClient_(std::move(nativeClient))
    , InstancePath_(std::move(instancePath))
{
    Banned_.store(false);
    StateCheckerExecutor_ = New<TPeriodicExecutor>(
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

bool TStateChecker::IsComponentBanned() const
{
    return Banned_.load();
}

////////////////////////////////////////////////////////////////////////////////

void TStateChecker::DoCheckState()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Started checking component state");
    auto logFinally = Finally([&] {
        YT_LOG_DEBUG("Finished checking component state (Banned: %v)", Banned_.load());
    });

    auto options = TGetNodeOptions{
        .Attributes = TAttributeFilter({BannedAttributeName}),
    };

    try {
        auto yson = WaitFor(NativeClient_->GetNode(InstancePath_, options))
            .ValueOrThrow();
        auto instance = ConvertToNode(yson);

        auto banned = false;
        if (instance->Attributes().Contains(BannedAttributeName)) {
            banned = instance->Attributes().Get<bool>(BannedAttributeName);
        }

        auto previousBanned = Banned_.exchange(banned);
        if (previousBanned != banned) {
            auto orchidBannedPath = InstancePath_ + "/orchid/@banned";

            YT_LOG_DEBUG("Updating orchid node (Path: %v, Banned: %v)", orchidBannedPath, banned);
            WaitFor(NativeClient_->SetNode(orchidBannedPath, ConvertToYsonString(banned)))
                .ThrowOnError();
            YT_LOG_DEBUG("Orchid node updated");
        }

    } catch (std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed checking component state");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NStateChecker
