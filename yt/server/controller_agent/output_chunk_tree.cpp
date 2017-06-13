#include "output_chunk_tree.h"

namespace NYT {
namespace NControllerAgent {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

void TBoundaryKeys::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, MinKey);
    Persist(context, MaxKey);
}

////////////////////////////////////////////////////////////////////////////////

TOutputChunkTreeKey::TOutputChunkTreeKey(int index)
    : Key_(index)
{ }

TOutputChunkTreeKey::TOutputChunkTreeKey(TBoundaryKeys boundaryKeys)
    : Key_(boundaryKeys)
{ }

TOutputChunkTreeKey::TOutputChunkTreeKey(TOutputOrder::TEntry Entry)
    : Key_(Entry)
{ }

TOutputChunkTreeKey::TOutputChunkTreeKey()
    : Key_(-1)
{ }

bool TOutputChunkTreeKey::IsIndex() const
{
    return Key_.template Is<int>();
}

bool TOutputChunkTreeKey::IsBoundaryKeys() const
{
    return Key_.template Is<TBoundaryKeys>();
}

bool TOutputChunkTreeKey::IsOutputOrderEntry() const
{
    return Key_.template Is<TOutputOrder::TEntry>();
}

int& TOutputChunkTreeKey::AsIndex()
{
    return Key_.template As<int>();
}

const int& TOutputChunkTreeKey::AsIndex() const
{
    return Key_.template As<int>();
}

TBoundaryKeys& TOutputChunkTreeKey::AsBoundaryKeys()
{
    return Key_.template As<TBoundaryKeys>();
}

const TBoundaryKeys& TOutputChunkTreeKey::AsBoundaryKeys() const
{
    return Key_.template As<TBoundaryKeys>();
}

TOutputOrder::TEntry& TOutputChunkTreeKey::AsOutputOrderEntry()
{
    return Key_.template As<TOutputOrder::TEntry>();
}

const TOutputOrder::TEntry& TOutputChunkTreeKey::AsOutputOrderEntry() const
{
    return Key_.template As<TOutputOrder::TEntry>();
}

void TOutputChunkTreeKey::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Key_);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TOutputChunkTreeKey& key)
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

} // namespace NControllerAgent
} // namespace NYT
