#include "key_info.h"

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

bool TKeyPairMetadata::IsValid() const noexcept
{
    // TODO(pavook): separate timestamp provider into interface.
    TInstant currentTime = Now();

    return ValidAfter < currentTime && currentTime < ExpiresAt;
}

////////////////////////////////////////////////////////////////////////////////

bool TKeyInfo::Verify(
    std::span<const std::byte> data,
    std::span<const std::byte, SignatureSize> signature) const noexcept
{
    return Meta().IsValid() && crypto_sign_verify_detached(
        reinterpret_cast<const unsigned char*>(signature.data()),
        reinterpret_cast<const unsigned char*>(data.data()),
        data.size(),
        reinterpret_cast<const unsigned char*>(Key().data())) == 0;
}

////////////////////////////////////////////////////////////////////////////////

TKeyInfo::TKeyInfo(const TPublicKey& key, const TKeyPairMetadata& meta) noexcept
    : Key_(key)
    , Meta_(meta)
{ }

////////////////////////////////////////////////////////////////////////////////

const TPublicKey& TKeyInfo::Key() const noexcept
{
    return Key_;
}

////////////////////////////////////////////////////////////////////////////////

const TKeyPairMetadata& TKeyInfo::Meta() const noexcept
{
    return Meta_;
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] bool TKeyInfo::operator==(const TKeyInfo& other) const noexcept
{
    return Key() == other.Key() && Meta() == other.Meta();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
