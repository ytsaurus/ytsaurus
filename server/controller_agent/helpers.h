#pragma once

#include "private.h"

#include "serialize.h"
#include "data_flow_graph.h"

#include <yt/server/chunk_pools/chunk_stripe_key.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/ytlib/scheduler/config.h>

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::INodePtr specNode);

NYTree::INodePtr UpdateSpec(NYTree::INodePtr templateSpec, NYTree::INodePtr originalSpec);

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
    // This field is used only during file size validation only for table chunks with column selectors.
    std::vector<NChunkClient::TInputChunkPtr> Chunks;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

NChunkPools::TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TEdgeDescriptor& outputTable,
    const NTableClient::TRowBufferPtr& rowBuffer);

void BuildFileSpecs(NScheduler::NProto::TUserJobSpec* jobSpec, const std::vector<TUserFile>& files);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TDataSourceDirectoryPtr BuildDataSourceDirectoryFromInputTables(const std::vector<TInputTablePtr>& inputTables);
NChunkClient::TDataSourceDirectoryPtr BuildIntermediateDataSourceDirectory();

void SetDataSourceDirectory(NScheduler::NProto::TSchedulerJobSpecExt* jobSpec, const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TAvgSummary
{
public:
    DEFINE_BYVAL_RO_PROPERTY(T, Sum);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);
    DEFINE_BYVAL_RO_PROPERTY(TNullable<T>, Avg);

public:
    TAvgSummary();
    TAvgSummary(T sum, i64 count);

    void AddSample(T sample);

    void Persist(const TPersistenceContext& context);

private:
    TNullable<T> CalcAvg();
};

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): move to ytlib.
NApi::NNative::IConnectionPtr FindRemoteConnection(
    const NApi::NNative::IConnectionPtr& connection,
    NObjectClient::TCellTag cellTag);

// TODO(ignat): move to ytlib.
NApi::NNative::IConnectionPtr GetRemoteConnectionOrThrow(
    const NApi::NNative::IConnectionPtr& connection,
    NObjectClient::TCellTag cellTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
