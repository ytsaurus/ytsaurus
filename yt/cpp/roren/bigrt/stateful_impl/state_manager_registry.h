#pragma once

#include "../fwd.h"

#include <util/generic/hash.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TStateManagerRegistry
    : public TThrRefBase
{
public:
    void Add(TString id, NBigRT::TBaseStateManagerPtr stateManager);
    const NBigRT::TBaseStateManagerPtr GetBase(TString id) const;

private:
    THashMap<TString, NBigRT::TBaseStateManagerPtr> Table_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
