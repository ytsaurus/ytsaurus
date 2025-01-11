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

TMangledSequoiaPath MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(
    const TMangledSequoiaPath& prefix);

//! Unescapes special characters.
TString ToStringLiteral(TStringBuf key);

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableSequoiaError(const TError& error);

bool IsRetriableSequoiaReplicasError(const TError& error);

bool IsMethodShouldBeHandledByMaster(const std::string& method);

////////////////////////////////////////////////////////////////////////////////

// NB: We want to use ApplyUnique() almost everywhere but TFuture<void> doesn't
// have this method. So |void| is a special case.
template <class T>
TErrorOr<T> MaybeWrapSequoiaRetriableError(
    std::conditional_t<std::is_void_v<T>, const TError&, TErrorOr<T>&&> result);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
