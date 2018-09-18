#pragma once

#include <yt/core/ypath/public.h>

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TNullable<i64> TryGetInt64(TStringBuf yson, const NYPath::TYPath& ypath);
TNullable<ui64> TryGetUint64(TStringBuf yson, const NYPath::TYPath& ypath);
TNullable<bool> TryGetBoolean(TStringBuf yson, const NYPath::TYPath& ypath);
TNullable<double> TryGetDouble(TStringBuf yson, const NYPath::TYPath& ypath);
TNullable<TString> TryGetString(TStringBuf yson, const NYPath::TYPath& ypath);
TNullable<TString> TryGetAny(TStringBuf yson, const NYPath::TYPath& ypath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
