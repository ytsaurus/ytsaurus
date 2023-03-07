#pragma once

#include "public.h"

#include <yt/core/misc/small_vector.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TSecurityTags
{
    TSecurityTagsItems Items;

    void Normalize();
    void Validate();

    bool IsEmpty() const;

    operator size_t() const;

    bool operator == (const TSecurityTags& rhs) const;
    bool operator != (const TSecurityTags& rhs) const;

    void Persist(TStreamPersistenceContext& context);
};

TSecurityTags operator + (const TSecurityTags& a, const TSecurityTags& b);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
