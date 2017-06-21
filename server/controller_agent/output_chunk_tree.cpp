#include "output_chunk_tree.h"

namespace NYT {
namespace NControllerAgent {

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

void TOutputChunkTreeKey::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Key_);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TOutputChunkTreeKey& key)
{
    if (key.IsIndex()) {
        return "index@" + ::ToString(key.AsIndex());
    } else if (key.IsBoundaryKeys()) {
        auto boundaryKeys = key.AsBoundaryKeys();
        return "bnd_keys@{" + ToString(boundaryKeys.MinKey) + ", " + ToString(boundaryKeys.MaxKey) + "}";
    } else {
        Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
