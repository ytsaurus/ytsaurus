#pragma once

#include <yt/yt/server/lib/signature/key_info.h>

#include <random>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TKeyPairMetadata SimpleMetadata(
    auto createdDelta,
    auto validAfterDelta,
    auto expiresAtDelta,
    TKeyId keyId = TKeyId(TGuid::Create()),
    TOwnerId ownerId = TOwnerId("test"))
{
    // TODO(pavook) mock time provider.

    auto now = Now();
    return TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .Owner = ownerId,
        .Id = keyId,
        .CreatedAt = now + createdDelta,
        .ValidAfter = now + validAfterDelta,
        .ExpiresAt = now + expiresAtDelta,
    };
}

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
