#include "skeleton.h"

#include <yt/yt/core/misc/string_builder.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using namespace re2;

static RE2 GuidPattern = RE2("[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+");
static RE2 PathPattern = RE2("//[^ ]*");
static RE2 AddressPattern = RE2("[a-z0-9-.]+.yp-c.yandex.net:[0-9]+");
static RE2 SemicolonPattern = RE2(";");

static std::vector<std::pair<RE2*, TString>> Replacements{
    {&GuidPattern, "<guid>"},
    {&PathPattern, "<path>"},
    {&AddressPattern, "<address>"},
    {&SemicolonPattern, ""},
};

TString GetErrorFingerprint(const TError& error)
{
    auto message = error.GetMessage();
    for (const auto& [pattern, substitution] : Replacements) {
        RE2::GlobalReplace(&message, *pattern, substitution);
    }

    TStringBuilder result;
    result.AppendFormat("#%v: %v", error.GetCode(), message);

    return result.Flush();
}

TString GetErrorSkeleton(const TError& error)
{
    std::vector<TString> innerSkeletons;
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
