#pragma once

#include "writer_storage.h"

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TStaticTableExportMessage
{
    i64 Offset{};
    //! The system timestamp of the message (the source write time); it selects the window.
    TSystemTimestamp WriteTimestamp;
};

struct TStaticTableExportParsedRows
{
    TSharedRange<NTableClient::TUnversionedRow> Rows;
    i64 MemoryUsage = 0;
    i64 RejectedRecords = 0;
};

//! Converts one source message to rows; runs on the pool invoker of the originating job, which owns it.
using TStaticTableExportRowParser = std::function<TStaticTableExportParsedRows(
    const TOutputMessageConstPtr& message,
    const NTableClient::TNameTablePtr& nameTable,
    int partition)>;

struct TStaticTableExportBinding
{
    NApi::IClientPtr Client;
    TStaticTableExportParametersPtr Parameters;
    NTableClient::TTableSchemaPtr Schema;
    NYTree::INodePtr Owner;
    std::string SourceIdentity;
    TStaticTableExportRowParser ParseRows;
};

struct TStaticTableExportPartitionState
{
    //! Delivered messages not yet acknowledged.
    i64 PendingMessageCount = 0;
    //! The durable state of the partition is known: it is claimed, or the export has no state yet.
    bool Claimed = false;
    //! The partition is claimed in the bootstrapped export.
    bool Bootstrapped = false;
    //! Some committed window of the partition is not published yet.
    bool Unpublished = false;
    //! The write time of the first message delivered to this participant, or its registration time if none came soon.
    std::optional<TSystemTimestamp> FirstWriteTimestamp;
    //! No message came and the job drains, so the partition does not constrain the start.
    bool SilentWhileDraining = false;
};

//! A job's registration with the worker writer; destruction drops unacknowledged deliveries.
struct IStaticTableExportParticipant
    : public virtual TRefCounted
{
    //! Blocks the calling fiber while the raw payload budget is exhausted.
    virtual void Enqueue(TStaticTableExportMessage metadata, TOutputMessageConstPtr message, TOnDistributedCallback callback) = 0;
    virtual void SetDraining(bool draining) = 0;
    //! Drops the unadmitted suffix and completes once the admitted prefix is committed.
    virtual TFuture<void> PrepareForHandoff() = 0;
    virtual void ThrowOnError() const = 0;
    virtual TStaticTableExportPartitionState GetState() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IStaticTableExportParticipant);

//! Appends the messages of every export job on a worker to the shared window tables.
class TStaticTableExportWriterResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TStaticTableExportWriterParameters);

    TStaticTableExportWriterResource(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);
    ~TStaticTableExportWriterResource() override;

    IStaticTableExportParticipantPtr RegisterPartition(
        TStaticTableExportBinding binding,
        TStaticTableExportClaim claim,
        IInvokerPtr poolInvoker);

private:
    class TWriter;
    const TIntrusivePtr<TWriter> Writer_;
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportWriterResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
