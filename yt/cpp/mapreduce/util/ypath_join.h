#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/strbuf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void AppendToYPath(TYPath& path) noexcept;
void AppendToYPathImpl(TYPath& path, TStringBuf arg);

template<typename... TStr>
void AppendToYPath(TYPath& path, TStringBuf arg, TStr&&... args)
{
    AppendToYPathImpl(path, arg);
    AppendToYPath(path, std::forward<TStr>(args)...);
}

} // namespace NDetail

// C++ version of yt.wrapper.ypath_join
template<typename... TStr>
TYPath JoinYPaths(TStr&&... args)
{
    TYPath path;
    NDetail::AppendToYPath(path, std::forward<TStr>(args)...);
    return path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
