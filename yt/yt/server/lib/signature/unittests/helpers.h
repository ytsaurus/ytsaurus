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

    explicit TRandomByteGenerator(auto seedValue)
        : Rnd(seedValue)
    { }

    std::byte operator()()
    {
        return static_cast<std::byte>(Rnd());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
