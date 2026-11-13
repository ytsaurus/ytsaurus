#pragma once

#include "publisher.h"
#include "static_table_export.h"
#include "writer_resource.h"
#include "writer_storage.h"

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Validates what every export requires from its passthrough computation:
//! one source, one output stream and the export sink, precise event watermarks by the |writeTimeColumn| milliseconds
//! and a worker-only writer resource.
void ValidateStaticTableExportComputation(
    const TComputationSpec& spec,
    const TStaticTableExportParametersPtr& parameters,
    const std::string& sinkClassName,
    const std::string& writeTimeColumn);

//! The identity of an export: its common parameters merged with |source|.
NYTree::INodePtr MakeStaticTableExportOwner(
    const NYPath::TYPath& pipelinePath,
    const TComputationId& computationId,
    const TStaticTableExportParametersPtr& parameters,
    const NYTree::IMapNodePtr& source);

//! The published offset ranges as a list of maps with "partition", "first_offset" and "next_offset".
NYTree::IListNodePtr BuildStaticTableExportPartitions(std::span<const TStaticTableExportRange> ranges);

////////////////////////////////////////////////////////////////////////////////

//! The sink of one source partition; it hands messages to the worker writer and holds a drain until publication.
class TStaticTableExportSinkBase
    : public TSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TStaticTableExportParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicStaticTableExportParameters);

    TStaticTableExportSinkBase(TSinkContextPtr context, TDynamicSinkContextPtr dynamicContext, int partition);

    void Init(IInitContextPtr initContext) final;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) final;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) final;
    void Commit() final;
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() const final;
    NYTree::IMapNodePtr GetPartitionStatus() const final;
    void BeforeGracefulShutdown() final;

protected:
    //! Everything but the client and the parameters, which the base fills.
    virtual TStaticTableExportBinding MakeBinding() = 0;
    virtual TStaticTableExportMessage GetExportMessage(const TOutputMessage& message) const = 0;
    virtual void ValidateDynamicSpec() const;

private:
    const int Partition_;
    IStaticTableExportParticipantPtr Participant_;

    bool IsDraining() const;
    bool IsFinishing() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Publishes the windows of the export once every partition has passed them.
class TStaticTableExportSinkControllerBase
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TStaticTableExportParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicStaticTableExportParameters);

    using TSinkControllerBase::TSinkControllerBase;

    void UpdatePartitionStatuses(const TSinkPartitionStatuses& statuses) final;
    std::optional<i64> GetReceiverChannelCount() final;

protected:
    struct TExportSource
    {
        std::string Identity;
        int PartitionCount = 0;
    };

    //! Advances the publication over the partitions of |source|.
    void UpdatePublication(const TExportSource& source);

    virtual NTableClient::TTableSchemaPtr GetExportSchema() const = 0;
    virtual NYTree::INodePtr MakeOwner() const = 0;
    //! The source partition of a job partition key, or null when the key belongs to another source.
    virtual std::optional<int> FindPartition(const TKey& partitionKey, const std::string& identity) const = 0;
    virtual NYTree::IAttributeDictionaryPtr BuildPublicationAttributes(std::span<const TStaticTableExportRange> ranges) const = 0;

private:
    TStaticTableExportPublisherPtr Publisher_;
    TSinkPartitionStatuses PartitionStatuses_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
