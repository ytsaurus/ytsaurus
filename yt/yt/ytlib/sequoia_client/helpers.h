#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TSelectRowsQuery
{
    std::vector<TString> WhereConjuncts;
    std::vector<TString> OrderBy;
    std::optional<int> Limit;
};

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(NYPath::TYPathBuf rawPath);

NYPath::TYPath DemangleSequoiaPath(const TMangledSequoiaPath& mangledPath);

TMangledSequoiaPath MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(const TMangledSequoiaPath& prefix);

//! Unescapes special characters.
TString ToStringLiteral(TStringBuf key);

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableSequoiaError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
