#pragma once

#include "private.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TMultiChunkPoolInputOptions
{
    void Persist(const TPersistenceContext& context);
};

struct TMultiChunkPoolOutputOptions
{
    bool HandleBlockedJobs = true;

    void Persist(const TPersistenceContext& context);
};

struct TMultiChunkPoolOptions
    : public TMultiChunkPoolInputOptions
    , public TMultiChunkPoolOutputOptions
{ };

////////////////////////////////////////////////////////////////////////////////

IMultiChunkPoolInputPtr CreateMultiChunkPoolInput(
    std::vector<IChunkPoolInputPtr> underlyingPools,
    TMultiChunkPoolInputOptions options = {});

IMultiChunkPoolOutputPtr CreateMultiChunkPoolOutput(
    std::vector<IChunkPoolOutputPtr> underlyingPools,
    TMultiChunkPoolOutputOptions options = {});

IMultiChunkPoolPtr CreateMultiChunkPool(
    std::vector<IChunkPoolPtr> underlyingPools,
    TMultiChunkPoolOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
