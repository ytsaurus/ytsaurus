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

TChunkStripeKey::TChunkStripeKey(TOutputOrder::TEntry Entry)
    : Key_(Entry)
{ }

TChunkStripeKey::TChunkStripeKey()
    : Key_(TUninitialized{})
{ }

bool TChunkStripeKey::IsBoundaryKeys() const
{
    return std::holds_alternative<TBoundaryKeys>(Key_);
}

bool TChunkStripeKey::IsOutputOrderEntry() const
{
    return std::holds_alternative<TOutputOrder::TEntry>(Key_);
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

TOutputOrder::TEntry& TChunkStripeKey::AsOutputOrderEntry()
{
    return std::get<TOutputOrder::TEntry>(Key_);
}

const TOutputOrder::TEntry& TChunkStripeKey::AsOutputOrderEntry() const
{
    return std::get<TOutputOrder::TEntry>(Key_);
}

void TChunkStripeKey::RegisterMetadata(auto&& registrar)
{
    // COMPAT(coteeq)
    using TLegacyKeyType = std::variant<int, TBoundaryKeys, TOutputOrder::TEntry>;
    registrar.template VirtualField<1>(
        "LegacyKey_",
        [] (TThis* this_, auto& context) {
            auto legacyKey = Load<TLegacyKeyType>(context);
            Visit(
                legacyKey,
                [&] (int index) {
                    YT_VERIFY(index == 0 || index == -1);
                    this_->Key_ = TUninitialized{};
                },
                [&] (TOutputOrder::TEntry entry) {
                    this_->Key_ = entry;
                },
                [&] (TBoundaryKeys boundaryKeys) {
                    this_->Key_ = boundaryKeys;
                });
        })
        .BeforeVersion(ESnapshotVersion::ChunkStripeKeyNoIndex)();

    PHOENIX_REGISTER_FIELD(2, Key_,
        .SinceVersion(ESnapshotVersion::ChunkStripeKeyNoIndex));
}

bool TChunkStripeKey::operator ==(const TChunkStripeKey& other) const
{
    return Key_ == other.Key_;
}

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
    if (key.IsOutputOrderEntry()) {
        builder->AppendFormat("%v" ,key.AsOutputOrderEntry());
        return;
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
