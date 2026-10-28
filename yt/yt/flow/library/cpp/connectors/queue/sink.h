#pragma once

#include "public.h"

#include "queue_info.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/client/api/queue_client.h>

#include <yt/yt/client/queue_client/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/nonblocking_batcher.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TSyncQueueSink
    : public TSyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TSyncQueueSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicSyncQueueSinkParameters);

    using TSinkController = TQueueSinkController;

    TSyncQueueSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

protected:
    const NLogging::TLogger Logger;

private:
    const NTableClient::TNameTablePtr NameTable_;
    const std::optional<int> FlowMetaColumn_;
    const std::optional<int> TabletIndexColumn_;
    const TTabletRouterPtr TabletRouter_;

private:
    void DoInit() final;
    void DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages) final;

    NTableClient::TNameTablePtr GenerateNameTable() const;
    TTabletRouterPtr CreateTabletRouter() const;
};

DEFINE_REFCOUNTED_TYPE(TSyncQueueSink);

////////////////////////////////////////////////////////////////////////////////

struct TMessageIdQueueSinkState
    : public NYTree::TYsonStruct
{
    std::string ProducerId;

    REGISTER_YSON_STRUCT(TMessageIdQueueSinkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMessageIdQueueSinkState);

////////////////////////////////////////////////////////////////////////////////

class TAsyncQueueWriter
    : public TRefCounted
{
public:
    using TSessionResult = NApi::TCreateQueueProducerSessionResult;

    struct TRequest
    {
        NTableClient::TUnversionedOwningRow Row;
        TClosure OnWritten;
        TMessageId MessageId;
    };

    class TRequestLimiter
    {
    public:
        TRequestLimiter(i64 maxCount, i64 maxSize);

        bool IsFull() const;
        void Add(const TRequest& request);

    private:
        i64 MaxCount_;
        i64 MaxSize_;
        i64 Count_ = 0;
        i64 Size_ = 0;
    };

public:
    TAsyncQueueWriter(
        TSinkContextPtr context,
        TAsyncQueueWriterParametersBasePtr parameters,
        std::vector<std::string> clusters,
        TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
        NTableClient::TNameTablePtr nameTable,
        IStatusErrorStatePtr errorState,
        NLogging::TLogger logger);

    void Init(IInitContextPtr initContext, bool atMostOnce);
    void Terminate();
    //! Calls |onWritten| inline on the serialized invoker on success; it must not throw.
    void Write(
        NTableClient::TUnversionedOwningRow row,
        TMessageId messageId,
        TClosure onWritten);
    TFuture<TMessageId> GetInitialMessageId() const;
    void Reconfigure(TDynamicAsyncQueueWriterParametersPtr dynamicParameters);
    void ThrowIfFatalError() const;

protected:
    const NLogging::TLogger Logger;

private:
    struct TClusterData
    {
        std::string Name;
        NApi::IClientPtr Client;
        TFuture<TSessionResult> SessionFuture;
        std::optional<TSessionResult> Session;
        NApi::TCreateQueueProducerSessionOptions SessionOptions;
        NQueueClient::TQueueProducerSequenceNumber PhysicalSequenceNumber;
        TMessageId LastMessageId;
    };

    const TSinkContextPtr Context_;
    const TAsyncQueueWriterParametersBasePtr Parameters_;
    const NTableClient::TNameTablePtr NameTable_;
    const IStatusErrorStatePtr ErrorState_;

    TAtomicIntrusivePtr<TDynamicAsyncQueueWriterParameters> DynamicParameters_;
    TMutableStateClient<TMessageIdQueueSinkState> State_;
    std::vector<TClusterData> Clusters_;
    ssize_t CurrentClusterIndex_ = 0;
    bool AtMostOnce_ = false;
    TIntrusivePtr<NConcurrency::TNonblockingBatcher<TRequest, TRequestLimiter>> Requests_;
    TFuture<void> Executor_;
    std::optional<TError> FatalError_;

private:
    void InitSession(const std::string& producerId);
    void StartSession(TClusterData* cluster, const std::string& producerId);
    const TSessionResult& GetSession(TClusterData* cluster, const std::string& producerId);
    bool TryExecuteIteration(const std::string& producerId, std::vector<TRequest>* requests);
    static void Execute(TWeakPtr<TAsyncQueueWriter> weakThis, std::string producerId);
    static TSessionResult CreateSessionWithRetries(
        TWeakPtr<TAsyncQueueWriter> weakThis,
        ssize_t clusterIndex,
        NQueueClient::TQueueProducerSessionId sessionId,
        NApi::TCreateQueueProducerSessionOptions options);
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueWriter);

////////////////////////////////////////////////////////////////////////////////

class TAsyncQueueSink
    : public TSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncQueueSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAsyncQueueSinkParameters);

    using TSinkController = TQueueSinkController;

    TAsyncQueueSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);
    ~TAsyncQueueSink() override;

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) override;
    void Commit() override;

protected:
    struct TPendingMessage
    {
        TOutputMessageConstPtr Message;
        TOnDistributedCallback OnDistributed;
    };

    NTableClient::TNameTablePtr NameTable_;
    std::optional<int> FlowMetaColumn_;
    TAsyncQueueWriterPtr Writer_;
    TMessageId LastSeenMessageId_;

    virtual NTableClient::TNameTablePtr GenerateNameTable() const;
    virtual void WriteMessages(std::vector<TPendingMessage> pending);
    virtual TAsyncQueueWriterPtr CreateWriter() const;

    bool TryAdmitMessage(
        TPendingMessage& item,
        const std::optional<TMessageId>& initialMessageId = std::nullopt);

    TAsyncQueueWriterParametersBasePtr GetWriterParameters() const;
    TDynamicAsyncQueueWriterParametersPtr GetDynamicWriterParameters() const;

    void DoTerminate() override;

private:
    const bool AtMostOnce_;
    const NConcurrency::TAsyncSemaphorePtr QueueSizeSemaphore_;

    TMutableStateClient<TMessageId> AtMostOnceWatermark_;
    std::vector<TPendingMessage> CurrentEpoch_;

    NTableClient::TUnversionedOwningRow BuildRow(const TOutputMessageConstPtr& message) const;
    void ReconfigureAtMostOnce();
    void TerminateWriter();
};

class TAsyncMultiClusterQueueSink
    : public TAsyncQueueSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncMultiClusterQueueSinkParameters);

    using TAsyncQueueSink::TAsyncQueueSink;
    using TSinkController = TMultiClusterQueueSinkController;

protected:
    TAsyncQueueWriterPtr CreateWriter() const override;
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueSink);
DEFINE_REFCOUNTED_TYPE(TAsyncMultiClusterQueueSink);

////////////////////////////////////////////////////////////////////////////////

class TQueueSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TQueueSinkControllerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicQueueSinkControllerParameters);

    TQueueSinkController(
        TSinkControllerContextPtr context,
        TDynamicSinkControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;
    std::optional<i64> GetReceiverChannelCount() override;

private:
    const NApi::IClientPtr Client_;
    const TQueueInfoControllerPtr Info_;
    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    IStatusErrorStatePtr WriteMetaErrorState_;

    void WriteFlowQueueMeta();
};

DEFINE_REFCOUNTED_TYPE(TQueueSinkController);

////////////////////////////////////////////////////////////////////////////////

class TMultiClusterQueueSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TQueueSinkControllerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicQueueSinkControllerParameters);

    TMultiClusterQueueSinkController(
        TSinkControllerContextPtr context,
        TDynamicSinkControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;
    std::optional<i64> GetReceiverChannelCount() override;

private:
    void ConstructByCluster(const std::string& cluster, const NApi::IClientPtr client);

private:
    THashMap<std::string, NApi::IClientPtr> ClientByCluster_;
    THashMap<std::string, TQueueInfoControllerPtr> InfoByCluster_;
    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    IStatusErrorStatePtr WriteMetaErrorState_;

    void WriteFlowQueueMeta();
};

DEFINE_REFCOUNTED_TYPE(TMultiClusterQueueSinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
