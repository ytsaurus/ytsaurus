#pragma once

#include "private.h"

#include "serialize.h"
#include "data_flow_graph.h"

#include <yt/server/chunk_pools/chunk_stripe_key.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

struct IJobSizeConstraints
    : public virtual TRefCounted
    , public virtual NPhoenix::IPersistent
{
    //! True if neither job count nor data weight per job were explicitly specified by user in spec.
    virtual bool CanAdjustDataWeightPerJob() const = 0;

    //! True if job count was explicitly specified by user in spec.
    virtual bool IsExplicitJobCount() const = 0;

    //! Job count, estimated from input statistics or provided via operation spec.
    virtual int GetJobCount() const = 0;

    //! Approximate data weight, estimated from input statistics or provided via operation spec.
    virtual i64 GetDataWeightPerJob() const = 0;

    //! Recommended upper limit on the number of chunk stripes per job.
    //! Can be overflown if exact job count is provided.
    virtual i64 GetMaxDataSlicesPerJob() const = 0;

    //! Recommended upper limit on the data size per job.
    //! Can be overflown if exact job count is provided.
    virtual i64 GetMaxDataWeightPerJob() const = 0;

    virtual i64 GetInputSliceDataWeight() const = 0;
    virtual i64 GetInputSliceRowCount() const = 0;

    //! Approximate primary data size. Has meaning only in context of sorted operation.
    virtual i64 GetPrimaryDataWeightPerJob() const = 0;

    virtual void Persist(const NPhoenix::TPersistenceContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

//! Fits for operations with user code.
IJobSizeConstraintsPtr CreateUserJobSizeConstraints(
    const NScheduler::TSimpleOperationSpecBasePtr& spec,
    const NScheduler::TSimpleOperationOptionsPtr& options,
    int outputTableCount,
    double dataWeightRatio,
    i64 primaryInputDataWeight,
    i64 inputRowCount = std::numeric_limits<i64>::max(),
    i64 foreignInputDataWeight = 0);

//! Fits for system operations like merge or erase.
IJobSizeConstraintsPtr CreateMergeJobSizeConstraints(
    const NScheduler::TSimpleOperationSpecBasePtr& spec,
    const NScheduler::TSimpleOperationOptionsPtr& options,
    i64 inputDataWeight,
    double dataWeightRatio,
    double compressionRatio);

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NScheduler::TSortOperationOptionsBasePtr& options,
    i64 inputDataWeight);

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NScheduler::TSortOperationOptionsBasePtr& options,
    i64 inputDataSize,
    i64 inputDataWeight,
    i64 inputRowCount,
    double compressionRatio);

IJobSizeConstraintsPtr CreatePartitionBoundSortedJobSizeConstraints(
    const NScheduler::TSortOperationSpecBasePtr& spec,
    const NScheduler::TSortOperationOptionsBasePtr& options,
    int outputTableCount);

IJobSizeConstraintsPtr CreateExplicitJobSizeConstraints(
    bool canAdjustDataSizePerJob,
    bool isExplicitJobCount,
    int jobCount,
    i64 dataWeightPerJob,
    i64 primaryDataWeightPerJob,
    i64 maxDataSlicesPerJob,
    i64 maxDataWeightPerJob,
    i64 inputSliceDataWeight,
    i64 inputSliceRowCount);

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::IMapNodePtr specNode);

////////////////////////////////////////////////////////////////////////////////

TString TrimCommandForBriefSpec(const TString& command);

////////////////////////////////////////////////////////////////////////////////

//! Common pattern in scheduler is to lock input object and
//! then request attributes of this object by id.
struct TLockedUserObject
    : public NChunkClient::TUserObject
{
    virtual TString GetPath() const override;
};

////////////////////////////////////////////////////////////////////////////////

struct TUserFile
    : public TLockedUserObject
{
    std::shared_ptr<NYTree::IAttributeDictionary> Attributes;
    TString FileName;
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
    i64 ChunkCount = -1;
    bool Executable = false;
    NYson::TYsonString Format;
    NTableClient::TTableSchema Schema;
    bool IsDynamic = false;
    bool IsLayer = false;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

NChunkPools::TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TEdgeDescriptor& outputTable,
    const NTableClient::TRowBufferPtr& rowBuffer);

void BuildFileSpecs(NScheduler::NProto::TUserJobSpec* jobSpec, const std::vector<TUserFile>& files);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
