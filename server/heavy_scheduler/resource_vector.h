#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Resource vector approximately represents pod resource requests and
//! node resources in the same linear space.
class TResourceVector
{
public:
    using TValue = ui64;
    static constexpr size_t DimensionCount = 7;

    explicit TResourceVector(std::array<TValue, DimensionCount> values);

    TValue& operator [] (size_t i);
    TValue operator [] (size_t i) const;

private:
    std::array<TValue, DimensionCount> Values_;
};

////////////////////////////////////////////////////////////////////////////////

TResourceVector& operator -= (TResourceVector& lhs, const TResourceVector& rhs);
TResourceVector operator - (TResourceVector lhs, const TResourceVector& rhs);

TResourceVector& operator += (TResourceVector& lhs, const TResourceVector& rhs);
TResourceVector operator + (TResourceVector lhs, const TResourceVector& rhs);

bool operator >= (const TResourceVector& lhs, const TResourceVector& rhs);
bool operator < (const TResourceVector& lhs, const TResourceVector& rhs);

////////////////////////////////////////////////////////////////////////////////

TResourceVector GetResourceRequestVector(NCluster::TPod* pod);
TResourceVector GetFreeResourceVector(NCluster::TNode* node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
