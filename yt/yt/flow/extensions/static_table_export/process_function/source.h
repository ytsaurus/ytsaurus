#pragma once

#include "public.h"

#include <yt/yt/flow/extensions/static_table_export/group_by_system_timestamp/sink.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! What the export takes from the source spec.
struct TStaticTableExportSourceInfo
{
    std::string WriteTimeColumn;
    //! Source fields of the export identity.
    NYTree::IMapNodePtr Owner;
};

//! The source of an export as the sink of one source partition sees it.
struct IStaticTableExportPartitionSource
    : public TRefCounted
{
    virtual int GetPartition() const = 0;
    virtual std::string GetIdentity() const = 0;
    virtual TStaticTableExportMessage GetExportMessage(const TOutputMessage& message) const = 0;
    //! Whether |message| is a malformed record the source kept, exported as a rejected record without rows.
    //! Throws if the export cannot keep |message|; runs before the rows are made, so the error drops only the partition.
    virtual bool IsRejected(const TOutputMessage& message) const = 0;
    virtual void ValidateDynamicSpec(const TComputationSpec& spec, const TDynamicComputationSpec& dynamicSpec) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IStaticTableExportPartitionSource);

struct TStaticTableExportSourceTopology
{
    std::string Identity;
    int PartitionCount = 0;
};

//! The source of an export as the sink controller sees it.
struct IStaticTableExportControllerSource
    : public TRefCounted
{
    virtual void Init(IInitContextPtr initContext) = 0;
    virtual void Sync() = 0;
    virtual void Commit() = 0;
    //! Null while the source topology is unknown.
    virtual std::optional<TStaticTableExportSourceTopology> GetTopology() const = 0;
    //! The partition of a source key, or null when the key belongs to another source.
    virtual std::optional<int> FindPartition(const TKey& sourceKey, const std::string& identity) const = 0;
    //! The name and the value of the table attribute that describes the published offset ranges.
    virtual std::pair<std::string, NYTree::INodePtr> BuildMetadata(std::span<const TStaticTableExportRange> ranges) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IStaticTableExportControllerSource);

struct TStaticTableExportSourceFactory
{
    //! Validates the spec of the source and of its stream.
    std::function<TStaticTableExportSourceInfo(const TComputationSpec& spec)> Inspect;
    std::function<IStaticTableExportPartitionSourcePtr(const TSinkContextPtr& context)> CreatePartitionSource;
    std::function<IStaticTableExportControllerSourcePtr(const TSinkControllerContextPtr& context, const NLogging::TLogger& logger)> CreateControllerSource;
};

//! Makes the source class |sourceClassName| exportable; called from static initializers.
void RegisterStaticTableExportSource(const std::string& sourceClassName, TStaticTableExportSourceFactory factory);

//! The factory of the single source of |spec|; throws if its class is not exportable.
const TStaticTableExportSourceFactory& GetStaticTableExportSource(const TComputationSpec& spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
