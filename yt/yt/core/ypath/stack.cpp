#include "stack.h"

#include <yt/yt/core/misc/cast.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ypath/token.h>

#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/string/string.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

void TYPathStack::Push(TString key)
{
    Items_.emplace_back(std::move(key));
}

void TYPathStack::Push(int index)
{
    Items_.push_back(index);
}

void TYPathStack::IncreaseLastIndex()
{
    YT_VERIFY(!Items_.empty());
    YT_VERIFY(std::holds_alternative<int>(Items_.back()));
    ++std::get<int>(Items_.back());
}

void TYPathStack::Pop()
{
    YT_VERIFY(!Items_.empty());
    Items_.pop_back();
}

bool TYPathStack::IsEmpty() const
{
    return Items_.empty();
}

TYPath TYPathStack::GetPath() const
{
    if (Items_.empty()) {
        return {};
    }
    TStringBuilder builder;
    for (const auto& item : Items_) {
        builder.AppendChar('/');
        Visit(item,
            [&] (const TString& string) {
                builder.AppendString(ToYPathLiteral(string));
            },
            [&] (int integer) {
                builder.AppendFormat("%v", integer);
            });
    }
    return builder.Flush();
}

TYPath TYPathStack::GetHumanReadablePath() const
{
    auto path = GetPath();
    if (path.empty()) {
        static const TYPath Root("(root)");
        return Root;
    }
    return path;
}

std::optional<TString> TYPathStack::TryGetStringifiedLastPathToken() const
{
    if (Items_.empty()) {
        return {};
    }
    return ToString(Items_.back());
}

TString TYPathStack::ToString(const TYPathStack::TEntry& item)
{
    return Visit(item,
        [&] (const TString& string) {
            return string;
        },
        [&] (int integer) {
            return ::ToString(integer);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
