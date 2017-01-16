#pragma once

#include "private.h"

#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IJobSizeConstraints
    : public virtual TRefCounted
    , public virtual NPhoenix::IPersistent
{
    //! True if neither job count nor data size per job were explicitly specified by user in spec.
    virtual bool CanAdjustDataSizePerJob() const = 0;

    //! True if job count was explicitly specified by user in spec.
    virtual bool IsExplicitJobCount() const = 0;

    //! Job count, estimated from input statistics or provided via operation spec.
    virtual int GetJobCount() const = 0;

    //! Approximate data size, estimated from input statistics or provided via operation spec.
    virtual i64 GetDataSizePerJob() const = 0;

    //! Recommended upper limit on the number of chunk stripes per job.
    //! Can be overflown if exact job count is provided.
    virtual i64 GetMaxChunkStripesPerJob() const = 0;

    //! Recommended upper limit on the data size per job.
    //! Can be overflown if exact job count is provided.
    virtual i64 GetMaxDataSizePerJob() const = 0;

    virtual i64 GetInputSliceDataSize() const = 0;
    virtual i64 GetInputSliceRowCount() const = 0;

    virtual void Persist(const NPhoenix::TPersistenceContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

IJobSizeConstraintsPtr CreateSimpleJobSizeConstraints(
    const TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    i64 inputDataSize,
    i64 inputRowCount = std::numeric_limits<i64>::max());

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    i64 inputDataSize);

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    i64 inputDataSize,
    i64 inputRowCount,
    double compressionRatio);

////////////////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer);
void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer);
NYson::TYsonString BuildInputPaths(
    const std::vector<NYPath::TRichYPath>& inputPaths,
    const TChunkStripeListPtr& inputStripeList,
    EOperationType operationType,
    EJobType jobType);

////////////////////////////////////////////////////////////////////////////////

Stroka TrimCommandForBriefSpec(const Stroka& command);

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::IMapNodePtr specNode);

////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const NJobTrackerClient::NProto::TJobResult& result);

////////////////////////////////////////////////////////////////////

Stroka MakeOperationCodicilString(const TOperationId& operationId);
TCodicilGuard MakeOperationCodicilGuard(const TOperationId& operationId);

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

////////////////////////////////////////////////////////////////////

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
