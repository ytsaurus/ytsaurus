#include "field_filter.h"

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NScheduler {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TFieldFilter::TFieldFilter()
    : Filter_{ParseFilterFromOptions({})}
{ }

TFieldFilter::TFieldFilter(const NYTree::IAttributeDictionaryPtr& options)
    : Filter_{ParseFilterFromOptions(options)}
{ }

bool TFieldFilter::IsFieldSuitable(TStringBuf field) const
{
    if (!Filter_) {
        return true;
    }

    return Filter_->contains(field);
}

std::optional<THashSet<TString>> TFieldFilter::ParseFilterFromOptions(
    const NYTree::IAttributeDictionaryPtr& options)
{
    if (!options) {
        return std::nullopt;
    }

    auto fields = options->Find<THashSet<TString>>("fields");
    if (!fields) {
        return std::nullopt;
    }

    return std::move(*fields);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
