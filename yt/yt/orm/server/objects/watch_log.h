#pragma once

#include "public.h"

#include "type_handler.h"

#include <util/generic/fwd.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TWatchLog
{
    TString ObjectName;
    TString Name;
    TObjectFilter Filter;
    THashSet<TString> Selector;
    TTagSet RequiredTags;
    TTagSet ExcludedTags;

    bool operator==(const TWatchLog& log) const = default;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateSelectors(
    const THashSet<TString>& selectors,
    const IObjectTypeHandler* typeHandler,
    bool abortOnFail);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
