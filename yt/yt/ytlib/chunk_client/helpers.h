#pragma once

#include "public.h"
#include "chunk_owner_ypath_proxy.h"
#include "chunk_service_proxy.h"
#include "block.h"
#include "session_id.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const THashSet<int>& GetMasterChunkMetaExtensionTagsFilter();
const THashSet<int>& GetSchedulerChunkMetaExtensionTagsFilter();

////////////////////////////////////////////////////////////////////////////////

void ValidateReplicationFactor(int replicationFactor);

NObjectClient::TCellTag PickChunkHostingCell(
    const NApi::NNative::IConnectionPtr& connection,
    const NLogging::TLogger& logger);

TSessionId CreateChunk(
    const NApi::NNative::IClientPtr& client,
    NObjectClient::TCellTag cellTag,
    const TMultiChunkWriterOptionsPtr& options,
    NObjectClient::TTransactionId transactionId,
    TChunkListId chunkListId,
    const NLogging::TLogger& logger);

//! Synchronously parses #fetchResponse, populates #nodeDirectory,
//! issues additional |LocateChunks| requests for foreign chunks.
//! The resulting chunk specs are appended to #chunkSpecs.
void ProcessFetchResponse(
    const NApi::NNative::IClientPtr& client,
    const TChunkOwnerYPathProxy::TRspFetchPtr& fetchResponse,
    NObjectClient::TCellTag fetchCellTag,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    int maxChunksPerLocateRequest,
    std::optional<int> rangeIndex,
    const NLogging::TLogger& logger,
    std::vector<NProto::TChunkSpec>* chunkSpecs,
    bool skipUnavailableChunks = false,
    NNodeTrackerClient::EAddressType addressType = NNodeTrackerClient::EAddressType::InternalRpc);

//! Synchronously fetches chunk specs from master,
//! waits for the result and processes the responses.
// XXX(babenko): YT-11825; passing -1 to chunkCount disables multi-fetch
std::vector<NProto::TChunkSpec> FetchChunkSpecs(
    const NApi::NNative::IClientPtr& client,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const TUserObject& userObject,
    const std::vector<TReadRange>& ranges,
    int chunkCount,
    int maxChunksPerFetch,
    int maxChunksPerLocateRequest,
    const std::function<void(const TChunkOwnerYPathProxy::TReqFetchPtr&)>& initializeFetchRequest,
    const NLogging::TLogger& logger,
    bool skipUnavailableChunks = false,
    NNodeTrackerClient::EAddressType addressType = NNodeTrackerClient::EAddressType::InternalRpc);

// XXX(ifsmirnov, max42): helper intended only for testing purposes.
std::vector<NProto::TChunkSpec> FetchTabletStores(
    const NApi::NNative::IClientPtr& client,
    const TUserObject& userObject,
    const std::vector<TReadRange>& ranges,
    const NLogging::TLogger& logger);

//! Synchronously invokes TChunkServiceProxy::AllocateWriteTargets.
//! Populates node directory with the returned node descriptors.
//! Throws if the server returns no replicas.
TChunkReplicaWithMediumList AllocateWriteTargets(
    const NApi::NNative::IClientPtr& client,
    TSessionId sessionId,
    int desiredTargetCount,
    int minTargetCount,
    std::optional<int> replicationFactorOverride,
    std::optional<TString> preferredHostName,
    const std::vector<TString>& forbiddenAddresses,
    const std::vector<TString>& allocatedAddresses,
    const NLogging::TLogger& logger);

//! Returns the cumulative error for the whole batch.
/*!
 *  If the envelope request fails then the corresponding error is returned.
 *  Otherwise, subresponses are examined and a cumulative error
 *  is constructed (with individual errors attached as inner).
 *  If all subresponses were successful then OK is returned.
 */
TError GetCumulativeError(const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError);

//! Locate all chunks passed in #chunkSpecList.
/*! Chunks from #chunkSpecList are updated to store information about active replicas.
 *  If nonnull #nodeDirectory is provided it is populated with information about these replicas.
 */
void LocateChunks(
    const NApi::NNative::IClientPtr& client,
    int maxChunksPerLocateRequest,
    const std::vector<NProto::TChunkSpec*>& chunkSpecList,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const NLogging::TLogger& logger,
    bool skipUnavailableChunks = false,
    NNodeTrackerClient::EAddressType addressType = NNodeTrackerClient::EAddressType::InternalRpc);

////////////////////////////////////////////////////////////////////////////////

i64 GetChunkDataWeight(const NProto::TChunkSpec& chunkSpec);
i64 GetChunkCompressedDataSize(const NProto::TChunkSpec& chunkSpec);
i64 GetChunkUncompressedDataSize(const NProto::TChunkSpec& chunkSpec);
i64 GetChunkReaderMemoryEstimate(const NProto::TChunkSpec& chunkSpec, TMultiChunkReaderConfigPtr config);

IChunkReaderPtr CreateRemoteReader(
    const NProto::TChunkSpec& chunkSpec,
    TErasureReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost);

IChunkReaderPtr CreateRemoteReaderThrottlingAdapter(
    TChunkId chunkId,
    const IChunkReaderPtr& underlyingReader,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler);

////////////////////////////////////////////////////////////////////////////////

struct TUserObject
{
    TUserObject() = default;
    explicit TUserObject(
        NYPath::TRichYPath path,
        std::optional<NObjectClient::TTransactionId> transactionId = {});

    // Input
    NYPath::TRichYPath Path;
    // TODO(babenko): consider making non-optional
    std::optional<NObjectClient::TTransactionId> TransactionId;

    // Input/output
    NObjectClient::TObjectId ObjectId;

    // Output
    NObjectClient::TCellTag ExternalCellTag = NObjectClient::InvalidCellTag;
    NTransactionClient::TTransactionId ExternalTransactionId;
    NObjectClient::EObjectType Type = NObjectClient::EObjectType::Null;
    NHydra::TRevision Revision = NHydra::NullRevision;
    NHydra::TRevision ContentRevision = NHydra::NullRevision;
    NHydra::TRevision AttributeRevision = NHydra::NullRevision;
    std::vector<TString> OmittedInaccessibleColumns;
    std::vector<NSecurityClient::TSecurityTag> SecurityTags;
    i64 ChunkCount = -1;

    std::optional<TString> Account;

    virtual ~TUserObject() = default;

    //! Returns |true| if TUserObject::ObjectId is non-null.
    bool IsPrepared() const;

    //! Returns the underlying path from #Path.
    const NYPath::TYPath& GetPath() const;

    //! Constructs a path from TUserObject::ObjectId.
    //! The instance must be prepared.
    TString GetObjectIdPath() const;

    //! For prepared instances, delegates to #GetObjectIdPath, otherwise returns #Path.
    TString GetObjectIdPathIfAvailable() const;

    void Persist(const TStreamPersistenceContext& context);
};

struct TGetUserObjectBasicAttributesOptions
    : public NApi::TMasterReadOptions
{
    bool SuppressAccessTracking = false;
    bool SuppressExpirationTimeoutRenewal = false;
    bool OmitInaccessibleColumns = false;
    bool PopulateSecurityTags = false;
};

void GetUserObjectBasicAttributes(
    const NApi::NNative::IClientPtr& client,
    const std::vector<TUserObject*>& objects,
    // TODO(babenko): consider removing
    NObjectClient::TTransactionId defaultTransactionId,
    const NLogging::TLogger& logger,
    NYTree::EPermission permission,
    const TGetUserObjectBasicAttributesOptions& options = {});

template <class T>
std::vector<TUserObject*> MakeUserObjectList(std::vector<T>& vector);
template <class T>
std::vector<TUserObject*> MakeUserObjectList(std::vector<TIntrusivePtr<T>>& vector);

////////////////////////////////////////////////////////////////////////////////

template <class TRpcPtr>
std::vector<TBlock> GetRpcAttachedBlocks(const TRpcPtr& rpc, bool validateChecksums = true);

template <class TRpcPtr>
void SetRpcAttachedBlocks(const TRpcPtr& rpc, const std::vector<TBlock>& blocks);

////////////////////////////////////////////////////////////////////////////////

i64 CalculateDiskSpaceUsage(
    int replicationFactor,
    i64 regularDiskSpace,
    i64 erasureDiskSpace);

////////////////////////////////////////////////////////////////////////////////

void DumpCodecStatistics(
    const TCodecStatistics& codecStatistics,
    const NYPath::TYPath& path,
    TStatistics* statistics);

///////////////////////////////////////////////////////////////////////////////

bool IsAddressLocal(const TString& address);

///////////////////////////////////////////////////////////////////////////////

//! Helper struct useful for further joining several data sources and data slices
//! with renumerating using #JoinDataSliceSourcePairs.
struct TDataSliceSourcePair
{
    TDataSourceDirectoryPtr DataSourceDirectory;
    std::vector<TDataSliceDescriptor> DataSliceDescriptors;
};

//! Join several sequences of data slice descriptors with their data sources properly
//! renumerating them to keep correspondence between data slices and data sources.
TDataSliceSourcePair JoinDataSliceSourcePairs(std::vector<TDataSliceSourcePair> pairs);

///////////////////////////////////////////////////////////////////////////////

//! Return the list of all the supported chunk features in this binary.
EChunkFeatures GetSupportedChunkFeatures();

//! Validate whether a client with set of supported features represented by bitmask
//! #supportedFeatures can process a chunk with features represented by bitmask
//! #chunkFeatures. Throws an error if client cannot process such a chunk.
void ValidateChunkFeatures(
    TChunkId chunkId,
    ui64 chunkFeatures,
    ui64 supportedChunkFeatures);

///////////////////////////////////////////////////////////////////////////////

struct TChunkWriterCounters
{
    TChunkWriterCounters() = default;

    explicit TChunkWriterCounters(const NProfiling::TProfiler& profiler);

    void Increment(
        const NProto::TDataStatistics& dataStatistics,
        const TCodecStatistics& codecStatistics,
        int replicationFactor);

    NProfiling::TCounter DiskSpace;
    NProfiling::TCounter DataWeight;
    NProfiling::TTimeCounter CompressionCpuTime;
};

///////////////////////////////////////////////////////////////////////////////

struct TAllyReplicasInfo
{
    NChunkClient::TChunkReplicaWithMediumList Replicas;
    NHydra::TRevision Revision = NHydra::NullRevision;

    Y_FORCE_INLINE explicit operator bool() const;

    // TODO(babenko): drop?
    static TAllyReplicasInfo FromChunkReplicas(
        const TChunkReplicaList& chunkReplicas,
        NHydra::TRevision revision = NHydra::NullRevision);
    static TAllyReplicasInfo FromChunkReplicas(
        const TChunkReplicaWithMediumList& chunkReplicas,
        NHydra::TRevision revision = NHydra::NullRevision);
};

void ToProto(
    NProto::TAllyReplicasInfo* protoAllyReplicas,
    const TAllyReplicasInfo& allyReplicas);
void FromProto(
    TAllyReplicasInfo* allyReplicas,
    const NProto::TAllyReplicasInfo& protoAllyReplicas);

void FormatValue(
    TStringBuilderBase* builder,
    const TAllyReplicasInfo& allyReplicas,
    TStringBuf spec);
TString ToString(
    const TAllyReplicasInfo& allyReplicas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
