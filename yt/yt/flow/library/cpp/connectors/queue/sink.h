#pragma once

#include "public.h"

#include "queue_info.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/delegating_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

#include <yt/yt/client/queue_client/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/queue_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

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

private:
    void DoInit() final;
    void DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages) final;

    NTableClient::TNameTablePtr GenerateNameTable() const;
};

DEFINE_REFCOUNTED_TYPE(TSyncQueueSink);

////////////////////////////////////////////////////////////////////////////////

class IAsyncQueueWriter
    : public TRefCounted
{
public:
    ~IAsyncQueueWriter() override = default;
    virtual void InitSession(const std::string& producerId) = 0;
    virtual TFuture<void> Write(const NTableClient::TUnversionedOwningRow& row) = 0;
    virtual void Reconfigure(TDynamicAsyncQueueWriterParametersPtr dynamicParameters) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncQueueWriter);

////////////////////////////////////////////////////////////////////////////////

class TAsyncQueueWriterBase
    : public IAsyncQueueWriter
{
public:
    using TRequest = std::tuple<NTableClient::TUnversionedOwningRow, TPromise<void>>;

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
    TAsyncQueueWriterBase(
        TSinkContextPtr context,
        TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
        NTableClient::TNameTablePtr nameTable,
        IStatusErrorStatePtr errorState,
        NLogging::TLogger logger);

    void InitSession(const std::string& producerId) override;
    TFuture<void> Write(const NTableClient::TUnversionedOwningRow& row) override;
    void Reconfigure(TDynamicAsyncQueueWriterParametersPtr dynamicParameters) override;

protected:
    const TSinkContextPtr Context_;
    TAtomicIntrusivePtr<TDynamicAsyncQueueWriterParameters> DynamicParameters_;

    const NLogging::TLogger Logger;

    const NTableClient::TNameTablePtr NameTable_;
    const IStatusErrorStatePtr ErrorState_;

    TIntrusivePtr<NConcurrency::TNonblockingBatcher<TRequest, TRequestLimiter>> Requests_;

    TFuture<void> Executor_;

protected:
    virtual bool TryExecuteIteration(const std::string& producerId, const std::vector<TRequest>& requests) = 0;

private:
    static void Execute(TWeakPtr<TAsyncQueueWriterBase> weakThis, std::string producerId);
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueWriterBase);

////////////////////////////////////////////////////////////////////////////////

class TAsyncQueueWriter
    : public TAsyncQueueWriterBase
{
public:
    TAsyncQueueWriter(
        TSinkContextPtr context,
        TAsyncQueueWriterParametersPtr parameters,
        TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
        NTableClient::TNameTablePtr nameTable,
        IStatusErrorStatePtr errorState,
        NLogging::TLogger logger);

protected:
    bool TryExecuteIteration(const std::string& producerId, const std::vector<TRequest>& requests) override;

private:
    const TAsyncQueueWriterParametersPtr Parameters_;

    const NApi::IClientPtr Client_;
    const NQueueClient::IProducerClientPtr ProducerClient_;

    NQueueClient::IProducerSessionPtr Session_;

private:
    void ResetSession();
    NQueueClient::IProducerSessionPtr GetSession(const std::string& producerId);
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueWriter);

////////////////////////////////////////////////////////////////////////////////

class TAsyncMultiClusterQueueWriter
    : public TAsyncQueueWriterBase
{
public:
    struct TClusterData
    {
        const NApi::IClientPtr Client;
        const NQueueClient::IProducerClientPtr ProducerClient;
        NQueueClient::IProducerSessionPtr Session;
    };

    using TClusterDataMap = THashMap<std::string, TClusterData>;

    TAsyncMultiClusterQueueWriter(
        TSinkContextPtr context,
        TAsyncMultiClusterQueueWriterParametersPtr parameters,
        TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
        NTableClient::TNameTablePtr nameTable,
        IStatusErrorStatePtr errorState,
        NLogging::TLogger logger);

protected:
    bool TryExecuteIteration(const std::string& producerId, const std::vector<TRequest>& requests) override;

private:
    const TAsyncMultiClusterQueueWriterParametersPtr Parameters_;

    TClusterDataMap DataByCluster_;
    std::ptrdiff_t CurrentClusterIndex_;

private:
    void ConstructByCluster(const std::string& cluster);
    TError TryExecuteIterationOnCluster(
        const std::string& producerId,
        const std::vector<TRequest>& requests,
        TClusterDataMap::iterator& it);
    NQueueClient::IProducerSessionPtr GetSession(const std::string& producerId, TClusterData& clusterData);
};

DEFINE_REFCOUNTED_TYPE(TAsyncMultiClusterQueueWriter);

////////////////////////////////////////////////////////////////////////////////

class TAsyncQueueSinkImpl
    : public TDelegatingAsyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncQueueSinkParametersBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAsyncQueueSinkParameters);

    TAsyncQueueSinkImpl(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    ~TAsyncQueueSinkImpl() override;

protected:
    virtual IAsyncQueueWriterPtr CreateWriter() = 0;
    const NTableClient::TNameTablePtr NameTable_;

private:
    const int SeqNoColumn_;
    const std::optional<int> FlowMetaColumn_;
    IAsyncQueueWriterPtr Writer_;

private:
    void DoInit(const std::string& producerId) override;
    NTableClient::TUnversionedOwningRow BuildRow(const TOutputMessageConstPtr& message, i64 seqNo) const;
    std::pair<TFuture<void>, ui64> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) override;
    NTableClient::TNameTablePtr GenerateNameTable() const;
};

class TAsyncQueueSink
    : public TAsyncQueueSinkImpl
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncQueueSinkParameters);

    using TAsyncQueueSinkImpl::TAsyncQueueSinkImpl;
    using TSinkController = TQueueSinkController;

protected:
    IAsyncQueueWriterPtr CreateWriter() override;
};

class TAsyncMultiClusterQueueSink
    : public TAsyncQueueSinkImpl
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncMultiClusterQueueSinkParameters);

    using TAsyncQueueSinkImpl::TAsyncQueueSinkImpl;
    using TSinkController = TMultiClusterQueueSinkController;

protected:
    IAsyncQueueWriterPtr CreateWriter() override;
};

DEFINE_REFCOUNTED_TYPE(TAsyncQueueSinkImpl);
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
