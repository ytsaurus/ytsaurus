#pragma once

#include "public.h"

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TSecurityTags
{
    TSecurityTagsItems Items;

    void Normalize();
    void Validate();

    bool IsEmpty() const;

    operator size_t() const;

    bool operator==(const TSecurityTags& other) const = default;

    void Persist(const TStreamPersistenceContext& context);
};

TSecurityTags operator + (const TSecurityTags& a, const TSecurityTags& b);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
