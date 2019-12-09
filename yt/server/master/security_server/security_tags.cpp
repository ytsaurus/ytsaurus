#include "security_tags.h"

#include <yt/client/security_client/helpers.h>

#include <yt/core/misc/hash.h>
#include <yt/core/misc/serialize.h>

#include <util/generic/algorithm.h>

namespace NYT::NSecurityServer {

using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

bool TSecurityTags::IsEmpty() const
{
    return Items.empty();
}

TSecurityTags::operator size_t() const
{
    size_t result = 0;
    for (const auto& tag : Items) {
        HashCombine(result, tag);
    }
    return result;
}

bool TSecurityTags::operator==(const TSecurityTags& rhs) const
{
    return Items == rhs.Items;
}

bool TSecurityTags::operator!=(const TSecurityTags& rhs) const
{
    return !(*this == rhs);
}

void TSecurityTags::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Items);
}

void TSecurityTags::Normalize()
{
    SortUnique(Items);
}

void TSecurityTags::Validate()
{
    for (const auto& tag : Items) {
        ValidateSecurityTag(tag);
    }
}

TSecurityTags operator + (const TSecurityTags& a, const TSecurityTags& b)
{
    if (a.Items.empty()) {
        return b;
    }
    if (b.Items.empty()) {
        return a;
    }
    TSecurityTags result;
    result.Items.reserve(a.Items.size() + b.Items.size());
    result.Items.insert(result.Items.end(), a.Items.begin(), a.Items.end());
    result.Items.insert(result.Items.end(), b.Items.begin(), b.Items.end());
    result.Normalize();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
