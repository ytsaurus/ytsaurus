#include "skeleton.h"

#include <yt/yt/core/misc/string_builder.h>

#include <library/cpp/yt/misc/global.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT {

using namespace re2;

////////////////////////////////////////////////////////////////////////////////

namespace {

YT_DEFINE_GLOBAL(const RE2, GuidPattern, "[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+");
YT_DEFINE_GLOBAL(const RE2, PathPattern, "//[^ ]*");
YT_DEFINE_GLOBAL(const RE2, AddressPattern, "[a-z0-9-.]+.yp-c.yandex.net:[0-9]+");
YT_DEFINE_GLOBAL(const RE2, SemicolonPattern, ";");
YT_DEFINE_GLOBAL(const RE2, KeyPattern, "([Kk]ey) \"[\\w-]+\"");
YT_DEFINE_GLOBAL(const RE2, TimestampPattern, "([Tt]imestamp) [[:xdigit:]]+");
YT_DEFINE_GLOBAL(const RE2, AccountPattern, "([Aa]ccount) \"[\\w-]+\"");
YT_DEFINE_GLOBAL(const RE2, AttributePattern, "([Aa]ttribute) \"[\\w-]+\"");
YT_DEFINE_GLOBAL(const RE2, ReferencePattern, "([Rr]eference) \"[\\w-]+\"");

using TReplacements = std::vector<std::pair<const RE2*, std::string>>;
YT_DEFINE_GLOBAL(const TReplacements, Replacements, {
    {&GuidPattern(), "<guid>"},
    {&PathPattern(), "<path>"},
    {&AddressPattern(), "<address>"},
    {&SemicolonPattern(), ""},
    {&KeyPattern(), "\\1 <key>"},
    {&TimestampPattern(), "\\1 <timestamp>"},
    {&AccountPattern(), "\\1 <account>"},
    {&AttributePattern(), "\\1 <attribute>"},
    {&ReferencePattern(), "\\1 <reference>"},
});

std::string GetErrorFingerprint(const TError& error)
{
    auto message = error.GetMessage();
    for (const auto& [pattern, substitution] : Replacements()) {
        RE2::GlobalReplace(&message, *pattern, substitution);
    }

    TStringBuilder result;
    result.AppendFormat("#%v: %v", error.GetCode(), message);

    return result.Flush();
}

} // namespace

std::string GetErrorSkeleton(const TError& error)
{
    std::vector<std::string> innerSkeletons;
    innerSkeletons.reserve(error.InnerErrors().size());
    for (const auto& innerError : error.InnerErrors()) {
        innerSkeletons.emplace_back(GetErrorSkeleton(innerError));
    }
    std::sort(innerSkeletons.begin(), innerSkeletons.end());
    innerSkeletons.erase(std::unique(innerSkeletons.begin(), innerSkeletons.end()), innerSkeletons.end());

    TStringBuilder result;
    result.AppendString(GetErrorFingerprint(error));

    if (!innerSkeletons.empty()) {
        result.AppendString(" @ [");

        bool isFirst = true;
        for (const auto& innerSkeleton : innerSkeletons) {
            if (!isFirst) {
                result.AppendString("; ");
            }
            isFirst = false;
            result.AppendString(innerSkeleton);
        }
        result.AppendChar(']');
    }

    return result.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
