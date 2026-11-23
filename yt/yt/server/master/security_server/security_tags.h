#pragma once

#include "public.h"

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

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

    // NB: Stream contexts make the type persistable via both snapshot and copy contexts.
    using TSaveContext = TStreamSaveContext;
    using TLoadContext = TStreamLoadContext;

    PHOENIX_DECLARE_TYPE(TSecurityTags, 0xfb482e6c);
};

TSecurityTags operator+(const TSecurityTags& a, const TSecurityTags& b);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
