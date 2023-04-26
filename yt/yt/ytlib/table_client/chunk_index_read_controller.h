#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkIndexReadController
    : public TRefCounted
{
    using TChunkFragmentRequest = NChunkClient::IChunkFragmentReader::TChunkFragmentRequest;

    struct TReadRequest
    {
        // The whole system block will be requested iff block cache is provided and
        // it considers corresponding system block type as active.
        // Otherwise controller will only request system block fragments within #FragmentSubrequests.
        std::vector<int> SystemBlockIndexes;

        std::vector<TChunkFragmentRequest> FragmentSubrequests;
    };

    struct TReadResponse
    {
        std::vector<NChunkClient::TBlock> SystemBlocks;

        std::vector<TSharedRef> Fragments;
    };

    virtual TReadRequest GetReadRequest() = 0;

    virtual void HandleReadResponse(TReadResponse response) = 0;

    virtual bool IsFinished() const = 0;

    virtual const std::vector<TVersionedRow>& GetResult() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkIndexReadController)

////////////////////////////////////////////////////////////////////////////////

struct TChunkIndexReadControllerTestingOptions
{
    //! Non-empty to override default start slot index computation.
    std::vector<int> KeySlotIndexes;

    //! Non-null to narrow down number of distinct fingerprints.
    std::optional<int> FingerprintDomainSize;
};

// NB: Requires chunk to have chunk index system block,
// which is determined via presence of a specific extension in chunk meta.
// #testingOptions is used for unittesting.
// System blocks will be searched for in #blockCache if corresponding block type is active.
IChunkIndexReadControllerPtr CreateChunkIndexReadController(
    NChunkClient::TChunkId chunkId,
    const TColumnFilter& columnFilter,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TSharedRange<TLegacyKey> keys,
    TKeyComparer keyComparer,
    const TTableSchemaPtr& tableSchema,
    TTimestamp timestamp,
    bool produceAllVersions,
    const NChunkClient::IBlockCachePtr& blockCache,
    std::optional<TChunkIndexReadControllerTestingOptions> testingOptions,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
