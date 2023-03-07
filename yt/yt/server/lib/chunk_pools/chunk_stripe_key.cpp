#include "chunk_stripe_key.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

void TBoundaryKeys::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, MinKey);
    Persist(context, MaxKey);
}

TBoundaryKeys::operator bool() const
{
    return static_cast<bool>(MinKey) && static_cast<bool>(MaxKey);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripeKey::TChunkStripeKey(int index)
    : Key_(index)
{ }

TChunkStripeKey::TChunkStripeKey(TBoundaryKeys boundaryKeys)
    : Key_(boundaryKeys)
{ }

TChunkStripeKey::TChunkStripeKey(TOutputOrder::TEntry Entry)
    : Key_(Entry)
{ }

TChunkStripeKey::TChunkStripeKey()
    : Key_(-1)
{ }

bool TChunkStripeKey::IsIndex() const
{
    return std::holds_alternative<int>(Key_);
}

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
    return !(IsIndex() && AsIndex() == -1);
}

int& TChunkStripeKey::AsIndex()
{
    return std::get<int>(Key_);
}

int TChunkStripeKey::AsIndex() const
{
    return std::get<int>(Key_);
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

void TChunkStripeKey::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Key_);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TChunkStripeKey& key)
{
    using NYT::ToString;

    if (key.IsIndex()) {
        return "index@" + ToString(key.AsIndex());
    } else if (key.IsBoundaryKeys()) {
        auto boundaryKeys = key.AsBoundaryKeys();
        return "bnd_keys@{" + ToString(boundaryKeys.MinKey) + ", " + ToString(boundaryKeys.MaxKey) + "}";
    } else if (key.IsOutputOrderEntry()) {
        return ToString(key.AsOutputOrderEntry());
    } else {
        YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
