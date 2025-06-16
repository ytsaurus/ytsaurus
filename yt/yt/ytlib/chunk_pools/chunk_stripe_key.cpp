#include "chunk_stripe_key.h"

namespace NYT::NChunkPools {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

void TBoundaryKeys::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, MinKey);
    PHOENIX_REGISTER_FIELD(2, MaxKey);
}

bool TBoundaryKeys::operator ==(const TBoundaryKeys& other) const
{
    return MinKey == other.MinKey && MaxKey == other.MaxKey;
}

TBoundaryKeys::operator bool() const
{
    return static_cast<bool>(MinKey) && static_cast<bool>(MaxKey);
}

PHOENIX_DEFINE_TYPE(TBoundaryKeys);

////////////////////////////////////////////////////////////////////////////////

TChunkStripeKey::TChunkStripeKey(TBoundaryKeys boundaryKeys)
    : Key_(boundaryKeys)
{ }

TChunkStripeKey::TChunkStripeKey(TOutputCookie cookie)
    : Key_(cookie)
{ }

TChunkStripeKey::TChunkStripeKey()
    : Key_(TUninitialized{})
{ }

bool TChunkStripeKey::IsBoundaryKeys() const
{
    return std::holds_alternative<TBoundaryKeys>(Key_);
}

bool TChunkStripeKey::IsOutputCookie() const
{
    return std::holds_alternative<TOutputCookie>(Key_);
}

TChunkStripeKey::operator bool() const
{
    return !std::holds_alternative<TUninitialized>(Key_);
}

TBoundaryKeys& TChunkStripeKey::AsBoundaryKeys()
{
    return std::get<TBoundaryKeys>(Key_);
}

const TBoundaryKeys& TChunkStripeKey::AsBoundaryKeys() const
{
    return std::get<TBoundaryKeys>(Key_);
}

TOutputCookie TChunkStripeKey::AsOutputCookie() const
{
    return std::get<TOutputCookie>(Key_);
}

void TChunkStripeKey::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Key_,
        .SinceVersion(ESnapshotVersion::DropOutputOrder));
}

bool TChunkStripeKey::operator ==(const TChunkStripeKey& other) const
{
    return Key_ == other.Key_;
}

void TChunkStripeKey::TUninitialized::Persist(const auto& /*context*/)
{ }

PHOENIX_DEFINE_TYPE(TChunkStripeKey);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TChunkStripeKey& key, TStringBuf /*spec*/)
{
    if (!key) {
        builder->AppendString("uninitialized");
        return;
    }
    if (key.IsBoundaryKeys()) {
        auto boundaryKeys = key.AsBoundaryKeys();
        builder->AppendFormat("bnd_keys@{%v, %v}", boundaryKeys.MinKey, boundaryKeys.MaxKey);
        return;
    }
    if (key.IsOutputCookie()) {
        builder->AppendFormat("%v", key.AsOutputCookie());
        return;
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
