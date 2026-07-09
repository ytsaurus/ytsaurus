#pragma once

#include "public.h"

#include "computation_controller.h"
#include "source.h"

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TExtendedSourcePartitionStatus
    : public NYTree::TYsonStruct
{
public:
    NYTree::IMapNodePtr PartitionStatus;
    EPartitionState PartitionState{};

    REGISTER_YSON_STRUCT(TExtendedSourcePartitionStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExtendedSourcePartitionStatus);

////////////////////////////////////////////////////////////////////////////////

struct TSourceControllerContextBase
    : public TComputationControllerContextBase
{
    TStreamId SourceStreamId;
    TSourceSpecPtr SourceSpec;
};

struct TSourceControllerContext
    : public TRefCounted
    , public TSourceControllerContextBase
{
};

DEFINE_REFCOUNTED_TYPE(TSourceControllerContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSourceControllerContext
    : public TRefCounted
{
    TDynamicSourceSpecPtr DynamicSourceSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicSourceControllerContext);

////////////////////////////////////////////////////////////////////////////////

struct ISourceController
    : public TRefCounted
    , public virtual TReconfigurable<TDynamicSourceControllerContext>
{
public:
    // Provide TParameter[Ptr] and TDynamicParameter[Ptr] aliases. They are types of specs `Parameters` fields.
    // These types are used in source registration for future parsing.
    // They may be shadowed by macroses YT_FLOW_EXTEND_[DYNAMIC_]PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(ISource::TParameters);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(ISource::TDynamicParameters);

    virtual void Init(IInitContextPtr initContext) = 0;
    virtual void Sync() = 0;
    virtual void Commit() = 0;

    virtual void ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses) = 0;
    virtual std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() = 0;
    virtual std::string GetGroup(const TKey& key) = 0;

    // TStreamTraverseData::Epoch is not filled by this method.
    virtual std::optional<TStreamTraverseDataPtr> GetFutureKeysStreamTraverseData() = 0;

    //! Fingerprint of the parameters that identify the physical source (cluster, path, topic, ...).
    //! It is embedded both in the partition keys and in the controller state path, so when it
    //! changes the partitions are recreated and the previous controller state is orphaned and
    //! reclaimed. Empty (the default) opts the source out of identity versioning.
    virtual std::string GetSourceIdentity() const
    {
        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(ISourceController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
