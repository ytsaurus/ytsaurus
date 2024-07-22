#include "misc.h"

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

TString JoinFilters(const std::vector<TString>& filters)
{
    if (filters.empty()) {
        return {};
    } else if (filters.size() == 1) {
        return filters[0];
    }

    TStringBuilder builder;
    TDelimitedStringBuilderWrapper wrapper(&builder, " AND ");
    for (const auto& filter : filters) {
        if (filter) {
            wrapper->AppendFormat("(%v)", filter);
        }
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
