#include "helpers.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TKeyPairMetadata SimpleMetadata(
    TDuration createdDelta,
    TDuration validAfterDelta,
    TDuration expiresAtDelta,
    TKeyId keyId,
    TOwnerId ownerId)
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

} // namespace NYT::NSignature
