#pragma once

#include "yt/server/master/cypress_server/public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <util/generic/map.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TSecurityTags
{
    std::vector<TString> Items;

    void Normalize();

    operator size_t() const;

    bool operator == (const TSecurityTags& rhs) const;
    bool operator != (const TSecurityTags& rhs) const;

    void Persist(TStreamPersistenceContext& context);
};

TSecurityTags operator + (const TSecurityTags& a, const TSecurityTags& b);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
