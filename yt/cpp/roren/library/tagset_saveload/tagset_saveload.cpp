#include "tagset_saveload.h"

#include <library/cpp/yt/memory/serialize.h>
#include <library/cpp/yt/small_containers/compact_vector.h>

#include <yt/yt/library/profiling/tag.h>

#include <util/generic/xrange.h>

template <typename TItem, size_t N>
struct TSerializer<NYT::TCompactVector<TItem, N>>:
    public TVectorSerializer<NYT::TCompactVector<TItem, N>>
{
};

void TSerializer<NYT::NProfiling::TTagSet>::Save(IOutputStream* out, const NYT::NProfiling::TTagSet& value) {
    ::Save(out, value.Parents());
    ::Save(out, value.Children());
    ::Save(out, value.Required());
    ::Save(out, value.Excluded());
    ::Save(out, value.Alternative());
    ::Save(out, value.Tags());
}

void TSerializer<NYT::NProfiling::TTagSet>::Load(IInputStream* in, NYT::NProfiling::TTagSet& value) {
#define LOAD_TAG_INDEX_LIST(variable) \
NYT::NProfiling::TTagIndexList variable; \
::Load(in, variable)

    LOAD_TAG_INDEX_LIST(parents);
    LOAD_TAG_INDEX_LIST(children);
    LOAD_TAG_INDEX_LIST(required);
    LOAD_TAG_INDEX_LIST(excluded);
    LOAD_TAG_INDEX_LIST(alternatives);

#undef LOAD_TAG_INDEX_LIST

    NYT::NProfiling::TTagList tags;
    ::Load(in, tags);

    auto requiredIterator = required.begin();
    auto excludedIterator = excluded.begin();

    for (auto index: xrange(tags.size())) {
        int parent = parents[index] == NYT::NProfiling::NoTagSentinel
            ? parents[index]
            : NYT::NProfiling::NoParent;

        if (requiredIterator != required.end() && *requiredIterator == index) {
            value.AddRequiredTag(tags[index], parent);
            ++requiredIterator;
        } else if (excludedIterator != excluded.end() && *excludedIterator == index) {
            value.AddExcludedTag(tags[index], parent);
            ++excludedIterator;
        } else if (alternatives[index] != NYT::NProfiling::NoTagSentinel) {
            value.AddAlternativeTag(tags[index], alternatives[index], parent);
        } else if (children[index] != NYT::NProfiling::NoTagSentinel) {
            if (parent != NYT::NProfiling::NoParent) {
                value.AddExcludedTag(tags[index], children[index]);
            } else {
                value.AddTagWithChild(tags[index], children[index]);
            }
        } else {
            value.AddTag(tags[index], parent);
        }
    }
}
