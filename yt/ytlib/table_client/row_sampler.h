#pragma once

#include "public.h"

#include <yt/core/misc/farm_hash.h>

#include <random>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IRowSampler
{
    virtual ~IRowSampler() = default;

    virtual bool ShouldTakeRow(i64 rowIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TPrng>
class TRowSampler
    : public IRowSampler
{
public:
    TRowSampler(double rate, ui64 seed)
        : Distribution_(rate)
        , Seed_(seed)
    { }

    virtual bool ShouldTakeRow(i64 rowIndex) override
    {
        auto rowSeed = GetRowSeed(rowIndex);
        Generator_.seed(rowSeed);
        return Distribution_(Generator_);
    }

protected:
    virtual ui64 GetRowSeed(i64 rowIndex) const
    {
        return rowIndex;
    }

    std::bernoulli_distribution Distribution_;
    ui64 Seed_;
    TPrng Generator_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TPrng>
class TFarmhashRowSampler
    : public TRowSampler<TPrng>
{
public:
    TFarmhashRowSampler(double rate, ui64 seed)
        : TRowSampler<TPrng>(rate, seed)
    { }

protected:
    using TRowSampler<TPrng>::Seed_;

    virtual ui64 GetRowSeed(i64 rowIndex) const override
    {
        return Seed_ ^ FarmFingerprint(rowIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IRowSampler> CreateChunkRowSampler(
    const NChunkClient::TChunkId& chunkId,
    double rate,
    ui64 seed);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
