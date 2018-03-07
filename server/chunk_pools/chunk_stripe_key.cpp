#include "chunk_stripe_key.h"

namespace NYT {
namespace NChunkPools {

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
    return Key_.template Is<int>();
}

bool TChunkStripeKey::IsBoundaryKeys() const
{
    return Key_.template Is<TBoundaryKeys>();
}

bool TChunkStripeKey::IsOutputOrderEntry() const
{
    return Key_.template Is<TOutputOrder::TEntry>();
}

TChunkStripeKey::operator bool() const
{
    return !(IsIndex() && AsIndex() == -1);
}

int& TChunkStripeKey::AsIndex()
{
    return Key_.template As<int>();
}

const int& TChunkStripeKey::AsIndex() const
{
    return Key_.template As<int>();
}

TBoundaryKeys& TChunkStripeKey::AsBoundaryKeys()
{
    return Key_.template As<TBoundaryKeys>();
}

const TBoundaryKeys& TChunkStripeKey::AsBoundaryKeys() const
{
    return Key_.template As<TBoundaryKeys>();
}

TOutputOrder::TEntry& TChunkStripeKey::AsOutputOrderEntry()
{
    return Key_.template As<TOutputOrder::TEntry>();
}

const TOutputOrder::TEntry& TChunkStripeKey::AsOutputOrderEntry() const
{
    return Key_.template As<TOutputOrder::TEntry>();
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
        Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
