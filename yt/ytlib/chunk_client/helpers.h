#pragma once

#include "public.h"
#include "chunk_owner_ypath_proxy.h"
#include "chunk_service_proxy.h"
#include "block.h"
#include "session_id.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/security_client/public.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/hydra/public.h>

#include <yt/client/api/client.h>

#include <yt/core/actions/public.h>

#include <yt/library/erasure/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/ytree/permission.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

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

//! Synchronously invokes TChunkServiceProxy::AllocateWriteTargets.
//! Populates #nodeDirectory with the returned node descriptors.
//! Throws if the server returns no replicas.
TChunkReplicaWithMediumList AllocateWriteTargets(
    const NApi::NNative::IClientPtr& client,
    TSessionId sessionId,
    int desiredTargetCount,
    int minTargetCount,
    std::optional<int> replicationFactorOverride,
    bool preferLocalHost,
    const std::vector<TString>& forbiddenAddresses,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const NLogging::TLogger& logger);

//! Returns the cumulative error for the whole batch.
/*!
 *  If the envelope request fails then the corresponding error is returned.
 *  Otherwise, subresponses are examined and a cumulative error
 *  is constructed (with individual errors attached as inner).
 *  If all subresponses were successful then OK is returned.
 */
TError GetCumulativeError(const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError);

//! Locate all chunks passed in |chunkSpecList|.
/*! Chunks from |chunkSpecList| are updated to store information about active replicas.
 *  If nonnull |nodeDirectory| is provided it is also updated to store information
 *  about these replicas.
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
i64 GetChunkUncompressedDataSize(const NProto::TChunkSpec& chunkSpec);
i64 GetChunkReaderMemoryEstimate(const NProto::TChunkSpec& chunkSpec, TMultiChunkReaderConfigPtr config);

IChunkReaderPtr CreateRemoteReader(
    const NProto::TChunkSpec& chunkSpec,
    TErasureReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    std::optional<NNodeTrackerClient::TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler);

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
    std::vector<TString> OmittedInaccessibleColumns;
    std::vector<NSecurityClient::TSecurityTag> SecurityTags;
    i64 ChunkCount = -1;

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

} // namespace NYT::NChunkClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
