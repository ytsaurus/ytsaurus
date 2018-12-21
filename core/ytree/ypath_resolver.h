#pragma once

#include <yt/core/ypath/public.h>

#include <yt/core/misc/optional.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TryGetInt64(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<ui64> TryGetUint64(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<bool> TryGetBoolean(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<double> TryGetDouble(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<TString> TryGetString(TStringBuf yson, const NYPath::TYPath& ypath);
std::optional<TString> TryGetAny(TStringBuf yson, const NYPath::TYPath& ypath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
