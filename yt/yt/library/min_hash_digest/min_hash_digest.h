#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <map>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMinHashDigest
    : public TRefCounted
{
public:
    TMinHashDigest() = default;

    bool IsInitialized() const;
    void Initialize(TSharedRef data);

    static TSharedRef Build(
        const std::map<TFingerprint, ui64>& writeMinHashes,
        const std::map<TFingerprint, ui64>& deleteTombstoneMinHashes);

private:
    static constexpr i32 FormatVersion = 1;

    bool Initialized_ = false;

    std::map<TFingerprint, ui64> WriteMinHashes_;
    std::map<TFingerprint, ui64> DeleteTombstoneMinHashes_;
};

DEFINE_REFCOUNTED_TYPE(TMinHashDigest)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
