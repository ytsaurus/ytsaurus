#include "queue_controller.h"

namespace NYT::NQueueAgent {

using namespace NHydra;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TQueueControllerBase
    : public IQueueController
{
protected:
    TQueueControllerBase(
        TQueueId queueId,
        TQueueTableRow queueRow,
        IInvokerPtr invoker)
        : QueueId_(std::move(queueId))
        , QueueRow_(std::move(queueRow))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger.WithTag("QueueId: %Qv", QueueId_))
    {
        YT_LOG_INFO("Queue controller instantiated");
    }

    TQueueId QueueId_;
    TQueueTableRow QueueRow_;
    IInvokerPtr Invoker_;

    NLogging::TLogger Logger;

    void BuildOrchid(TFluentMap fluent) const override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        fluent
            .Item("type").Value(GetQueueType())
            .Item("row").Value(QueueRow_);
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();
    }

    TFuture<void> Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return VoidFuture;
    }

    IInvokerPtr GetInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Invoker_;
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueControllerBase)

class TOrderedDynamicTableController
    : public TQueueControllerBase
{
public:
    TOrderedDynamicTableController(
        TQueueId queueId,
        TQueueTableRow queueRow,
        IInvokerPtr invoker)
        : TQueueControllerBase(
            std::move(queueId),
            std::move(queueRow),
            std::move(invoker))
    { }

    EQueueType GetQueueType() const override
    {
        return EQueueType::OrderedDynamicTable;
    }
};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicTableController)

////////////////////////////////////////////////////////////////////////////////

IQueueControllerPtr CreateQueueController(
    TQueueId queueId,
    EQueueType queueType,
    TQueueTableRow queueRow,
    IInvokerPtr invoker)
{
    switch (queueType) {
        case EQueueType::OrderedDynamicTable:
            return New<TOrderedDynamicTableController>(std::move(queueId), std::move(queueRow), std::move(invoker));
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
