#pragma once

#include <yt/yt/server/lib/signature/key_info.h>

#include <random>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TKeyPairMetadata SimpleMetadata(
    TDuration createdDelta,
    TDuration validAfterDelta,
    TDuration expiresAtDelta,
    TKeyId keyId = TKeyId(TGuid::Create()),
    TOwnerId ownerId = TOwnerId("test"));

////////////////////////////////////////////////////////////////////////////////

struct TRandomByteGenerator
{
    std::mt19937 Rnd;

    TRandomByteGenerator() = default;

    char operator()()
    {
        return std::uniform_int_distribution<i8>()(Rnd);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
