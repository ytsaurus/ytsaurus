#include "queue_info.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void TQueueInfoControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("cached_partition_count", &TThis::CachedPartitionCount)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TQueueInfoController::TQueueInfoController(
    TQueueInfoSpecPtr spec,
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    const IStatusProfilerPtr statusProfiler)
    : Logger(logger)
    , Spec_(std::move(spec))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
    , ErrorState_(statusProfiler->ErrorState("/update_partition_count"))
    , Executor_()
{
}

void TQueueInfoController::Init(IInitContextPtr initContext)
{
    initContext->InitClient<TQueueInfoControllerState>(State_, "v0");
    Executor_ = New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TQueueInfoController::TryUpdatePartitionCount, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(Spec_->UpdatePartitionCountPeriod));
    Executor_->Start();
    Executor_->ScheduleOutOfBand();
}

void TQueueInfoController::Sync()
{ }

void TQueueInfoController::Commit()
{ }

void TQueueInfoController::TryUpdatePartitionCount()
{
    try {
        TGetNodeOptions options;
        options.Attributes = {"tablet_count", "type"};
        auto ysonString = WaitFor(Client_->GetNode(Spec_->QueuePath.GetPath(), options))
            .ValueOrThrow();
        auto node = NYTree::ConvertToNode(ysonString);
        const auto& attributes = node->Attributes();
        auto type = attributes.Get<NObjectClient::EObjectType>("type");
        if (type == NObjectClient::EObjectType::ChaosReplicatedTable) {
            THROW_ERROR_EXCEPTION("No support for chaos yet")
                << TErrorAttribute("queue_path", Spec_->QueuePath);
        }
        State_->CachedPartitionCount = attributes.Get<int>("tablet_count");
        YT_TLOG_INFO("Queue topic partition count was updated")
            .With("CurrentPartitionCount", State_->CachedPartitionCount);
        ErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to update partition count") << ex;
        ErrorState_->SetError(error);
    }
}

std::optional<int> TQueueInfoController::GetPartitionCount()
{
    return State_->CachedPartitionCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
