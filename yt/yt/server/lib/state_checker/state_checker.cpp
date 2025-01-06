#include "private.h"
#include "public.h"
#include "state_checker.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NComponentStateChecker {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TComponentStateChecker
    : public IComponentStateChecker
{
public:
    TComponentStateChecker(
        IInvokerPtr invoker,
        NApi::IClientPtr nativeClient,
        NYPath::TYPath instancePath,
        TDuration stateCheckPeriod)
        : Logger(ComponentStateCheckerLogger())
        , Invoker_(std::move(invoker))
        , NativeClient_(std::move(nativeClient))
        , InstancePath_(std::move(instancePath))
        , ComponentStateCheckerExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TComponentStateChecker::DoCheckState, MakeWeak(this)),
            stateCheckPeriod))
    { }

    void Start() override
    {
        ComponentStateCheckerExecutor_->Start();
    }

    void SetPeriod(TDuration stateCheckPeriod) override
    {
        ComponentStateCheckerExecutor_->SetPeriod(stateCheckPeriod);
    }

    bool IsComponentBanned() const override
    {
        return Banned_.load();
    }

    NYTree::IYPathServicePtr GetOrchidService() const override
    {
        auto producer = BIND(&TComponentStateChecker::DoBuildOrchid, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

private:
    const NLogging::TLogger Logger;

    const IInvokerPtr Invoker_;
    const NApi::IClientPtr NativeClient_;
    const NYPath::TYPath InstancePath_;
    const NConcurrency::TPeriodicExecutorPtr ComponentStateCheckerExecutor_;

    std::atomic<bool> Banned_ = false;

    void DoCheckState()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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

            Banned_.store(
                instance->Attributes().Get<bool>(BannedAttributeName, false));
        } catch (std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed checking component state");
        }
    }

    void DoBuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("banned").Value(IsComponentBanned())
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IComponentStateCheckerPtr CreateComponentStateChecker(
    IInvokerPtr invoker,
    NApi::IClientPtr nativeClient,
    NYPath::TYPath instancePath,
    TDuration stateCheckPeriod)
{
    return New<TComponentStateChecker>(
        std::move(invoker),
        std::move(nativeClient),
        std::move(instancePath),
        std::move(stateCheckPeriod));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComponentStateChecker
