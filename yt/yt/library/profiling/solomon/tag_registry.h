#pragma once

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/profiling/tag.h>

#include <util/generic/hash_set.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TTagRegistry
{
public:
    TTagIdList Encode(const TTagSet& tags);
    TTagIdList Encode(const TTagList& tags);

    //! TryEncode returns null if tags contains an unknown tag.
    std::optional<TTagIdList> TryEncode(const TTagList& tags) const;

    const TTag& Decode(TTagId tagId) const;
    int GetSize() const;
    THashMap<TString, int> TopByKey() const;

    TTagIdList EncodeLegacy(const TTagIdList& tagIds);

private:
    // TODO(prime@): maybe do something about the fact that tags are never freed.
    THashMap<TTag, TTagId> TagByName_;
    std::deque<TTag> TagById_;

    THashMap<TTagId, TTagId> LegacyTags_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
