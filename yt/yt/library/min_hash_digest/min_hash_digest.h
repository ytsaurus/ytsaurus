#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/yson/public.h>

#include <map>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMinHashDigest
    : public TRefCounted
{
public:
    explicit TMinHashDigest(IMemoryUsageTrackerPtr memoryTracker);

    ~TMinHashDigest();

    bool IsInitialized() const;
    void Initialize(TSharedRef data);

    i64 GetWeight() const;

    static TSharedRef Build(
        const std::map<TFingerprint, ui64>& writeMinHashes,
        const std::map<TFingerprint, ui64>& deleteTombstoneMinHashes);

private:
    static constexpr i32 FormatVersion = 1;

    bool Initialized_ = false;

    std::map<TFingerprint, ui64> WriteMinHashes_;
    std::map<TFingerprint, ui64> DeleteTombstoneMinHashes_;

    IMemoryUsageTrackerPtr MemoryTracker_;
};

DEFINE_REFCOUNTED_TYPE(TMinHashDigest)

void Serialize(const TMinHashDigest& minHashDigest, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
