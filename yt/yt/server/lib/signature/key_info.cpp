#include "key_info.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

bool TKeyPairMetadata::IsValid() const
{
    // TODO(pavook): separate timestamp provider into interface.
    auto currentTime = Now();

    return ValidAfter < currentTime && currentTime < ExpiresAt;
}

////////////////////////////////////////////////////////////////////////////////

bool TKeyInfo::Verify(
    std::span<const std::byte> data,
    std::span<const std::byte, SignatureSize> signature) const
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

const TPublicKey& TKeyInfo::Key() const
{
    return Key_;
}

////////////////////////////////////////////////////////////////////////////////

const TKeyPairMetadata& TKeyInfo::Meta() const
{
    return Meta_;
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] bool TKeyInfo::operator==(const TKeyInfo& other) const
{
    return Key() == other.Key() && Meta() == other.Meta();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
