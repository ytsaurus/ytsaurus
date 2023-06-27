#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/misc/common.h>

#pragma once

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! This class fetches chunk specs from master and processes the responses
//! (possibly locating foreign chunks).
class TMasterChunkSpecFetcher
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<NProto::TChunkSpec>, ChunkSpecs);

public:
    TMasterChunkSpecFetcher(
        const NApi::NNative::IClientPtr& client,
        const NApi::TMasterReadOptions& masterReadOptions,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const IInvokerPtr& invoker,
        int maxChunksPerFetch,
        int maxChunksPerLocateRequest,
        const std::function<void(const TChunkOwnerYPathProxy::TReqFetchPtr&, int /*tableIndex*/)>& initializeFetchRequest,
        const NLogging::TLogger& logger,
        bool skipUnavailableChunks = false);

    void Add(
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag externalCellTag,
        i64 chunkCount,
        int tableIndex = 0,
        const std::vector<TReadRange>& ranges = {TReadRange()});

    TFuture<void> Fetch();

    //! Returns fetched chunk specs ordered by table index.
    std::vector<NProto::TChunkSpec> GetChunkSpecsOrderedNaturally() const;

    NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const;

private:
    NApi::NNative::IClientPtr Client_;
    NApi::TMasterReadOptions MasterReadOptions_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    IInvokerPtr Invoker_;
    int MaxChunksPerFetch_;
    int MaxChunksPerLocateRequest_;
    std::function<void(const TChunkOwnerYPathProxy::TReqFetchPtr&, int)> InitializeFetchRequest_;
    NLogging::TLogger Logger;
    bool SkipUnavailableChunks_;
    i64 TotalChunkCount_ = 0;
    int TableCount_ = 0;

    struct TCellState
    {
        NObjectClient::TObjectServiceProxy::TReqExecuteBatchWithRetriesPtr BatchReq;
        int ReqCount = 0;
        std::vector<int> TableIndices;
        std::vector<int> RangeIndices;
        std::vector<NProto::TChunkSpec> ChunkSpecs;
        std::vector<NProto::TChunkSpec*> ForeignChunkSpecs;
    };

    THashMap<NObjectClient::TCellTag, TCellState> CellTagToState_;

    TCellState& GetCellState(NObjectClient::TCellTag cellTag);

    void DoFetch();
    void DoFetchFromCell(NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TMasterChunkSpecFetcher)

////////////////////////////////////////////////////////////////////////////////

//! This class fetches dynamic table chunk specs directly from tablet cells
//! also optionally retrieving sample keys.
//! Restrictions:
//! - fetcher currently works only with sorted dynamic tables;
//! - no retries are performed at all (i.e. when some tablets are missing, they
//!   are invalidated in tablet mount cache but no retry after invalidation happens).
class TTabletChunkSpecFetcher
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<NProto::TChunkSpec>, ChunkSpecs);

    using TRequest = NQueryClient::NProto::TReqFetchTabletStores;
    using TSubrequest = NQueryClient::NProto::TReqFetchTabletStores::TSubrequest;

public:
    struct TOptions
    {
        NApi::NNative::IClientPtr Client;
        //! Row buffer that is used when samples are requested.
        //! May be nullptr only if no samples are requested.
        NTableClient::TRowBufferPtr RowBuffer;
        //! This callback will be invoked for each request to particular node.
        //! It may be used to specify required extension tags or for something else.
        std::function<void(TRequest*)> InitializeFetchRequest;
        //! Codec used for compressing RPC responses.
        NCompression::ECodec ResponseCodecId = NCompression::ECodec::Lz4;
    };

    TTabletChunkSpecFetcher(
        TOptions options,
        const IInvokerPtr& invoker,
        const NLogging::TLogger& logger);

    void Add(
        const NYPath::TYPath& path,
        i64 chunkCount,
        int tableIndex = 0,
        const std::vector<TReadRange>& ranges = {TReadRange()});

    TFuture<void> Fetch();

private:
    const TOptions Options_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger& Logger;

    i64 TotalChunkCount_ = 0;
    int TableCount_ = 0;

    //! Used only for logging.
    static constexpr const int MissingTabletIdCountLimit = 5;

    struct TNodeState
    {
        //! Each subrequest consists of a bunch of ranges of single tablet.
        //! This vector stores subrequests for all tablets residing on this node.
        std::vector<TSubrequest> Subrequests;
        //! Tablet infos are stored for possible further invalidation in table mount cache
        //! (in case when subrequest fails with NTabletClient::EErrorCode::NoSuchTablet error).
        std::vector<NTabletClient::TTabletInfoPtr> Tablets;
        std::vector<NProto::TChunkSpec> ChunkSpecs;
        std::vector<NTabletClient::TTabletId> MissingTabletIds;
    };

    THashMap<TString, TNodeState> NodeAddressToState_;

    void AddSorted(
        const NTabletClient::TTableMountInfo& tableMountInfo,
        int tableIndex,
        const std::vector<TReadRange>& ranges);

    void DoFetch();
    void DoFetchFromNode(const TString& address);
};

DEFINE_REFCOUNTED_TYPE(TTabletChunkSpecFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
