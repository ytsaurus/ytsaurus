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
        TCrossClusterReference queueRef,
        TQueueTableRow queueRow,
        THashMap<TCrossClusterReference, TConsumerTableRow> consumerRefToRow,
        IInvokerPtr invoker)
        : QueueRef_(std::move(queueRef))
        , QueueRow_(std::move(queueRow))
        , ConsumerRefToRow_(std::move(consumerRefToRow))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger.WithTag("Queue: %Qv", QueueRef_))
    {
        YT_LOG_INFO("Queue controller instantiated");
    }

    TCrossClusterReference QueueRef_;
    TQueueTableRow QueueRow_;
    THashMap<TCrossClusterReference, TConsumerTableRow> ConsumerRefToRow_;
    IInvokerPtr Invoker_;

    NLogging::TLogger Logger;

    void BuildOrchid(TFluentMap fluent) const override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        fluent
            .Item("type").Value(GetQueueType())
            .Item("row").Value(QueueRow_);
    }

    void BuildConsumerOrchid(const TCrossClusterReference& consumerRef, TFluentMap fluent) const override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        fluent
            .Item("row").Value(GetOrCrash(ConsumerRefToRow_, consumerRef));
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller started");
    }

    TFuture<void> Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller stopped");

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
        TCrossClusterReference queueRef,
        TQueueTableRow queueRow,
        THashMap<TCrossClusterReference, TConsumerTableRow> consumerRefToRow,
        IInvokerPtr invoker)
        : TQueueControllerBase(
            std::move(queueRef),
            std::move(queueRow),
            std::move(consumerRefToRow),
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
    TCrossClusterReference queueRef,
    EQueueType queueType,
    TQueueTableRow queueRow,
    THashMap<TCrossClusterReference, TConsumerTableRow> consumerRefToRow,
    IInvokerPtr invoker)
{
    switch (queueType) {
        case EQueueType::OrderedDynamicTable:
            return New<TOrderedDynamicTableController>(
                std::move(queueRef),
                std::move(queueRow),
                std::move(consumerRefToRow),
                std::move(invoker));
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
