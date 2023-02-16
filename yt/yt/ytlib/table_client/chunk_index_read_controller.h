#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkIndexReadController
    : public TRefCounted
{
    using TChunkFragmentRequest = NChunkClient::IChunkFragmentReader::TChunkFragmentRequest;

    virtual std::vector<TChunkFragmentRequest> GetFragmentRequests() = 0;

    virtual void HandleFragmentsResponse(std::vector<TSharedRef> fragments) = 0;

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
IChunkIndexReadControllerPtr CreateChunkIndexReadController(
    NChunkClient::TChunkId chunkId,
    const TColumnFilter& columnFilter,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TSharedRange<TLegacyKey> keys,
    TKeyComparer keyComparer,
    const TTableSchemaPtr& tableSchema,
    TTimestamp timestamp,
    bool produceAllVersions,
    std::optional<TChunkIndexReadControllerTestingOptions> testingOptions,
    NLogging::TLogger logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
