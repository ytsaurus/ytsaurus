#pragma once

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TIndexedYsonString)

//! Read-only path lookup over a yson document (binary or text): upper map levels are parsed into
//! nodes, while subtrees no larger than |leafSizeThreshold| are kept as raw yson and parsed on demand.
//! Bounds the live-node count (unlike a full DOM) and avoids the O(document) scan of a flat by-path
//! extraction.
class TIndexedYsonString
    : public TRefCounted
{
public:
    struct TStats
    {
        i64 NodeCount = 0;
        i64 LeafCount = 0;
        i64 LeafBytes = 0;
        i64 MaxLeafBytes = 0;
    };

    TIndexedYsonString() = default;

    static TIndexedYsonStringPtr Build(NYson::TYsonString yson, i64 leafSizeThreshold);

    //! Returns the subtree at |path| (map-key segments) as a yson string; throws if absent.
    NYson::TYsonString GetByPath(const NYPath::TYPath& path) const;

    TStats ComputeStats() const;

private:
    bool IsLeaf_ = true;
    NYson::TYsonString Leaf_;
    THashMap<std::string, TIndexedYsonStringPtr> Children_;

    static TIndexedYsonStringPtr BuildValue(
        const TSharedRef& buffer,
        i64 valueBegin,
        i64 leafSizeThreshold,
        i64* valueEnd);
    void SerializeTo(NYson::IYsonConsumer* consumer) const;
};

DEFINE_REFCOUNTED_TYPE(TIndexedYsonString)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
