#pragma once

#include <yt/core/yson/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TStrace
    : public NYTree::TYsonSerializableLite
{
    Stroka Trace;
    Stroka ProcessName;
    std::vector<Stroka> ProcessCommandLine;

    TStrace();
};

////////////////////////////////////////////////////////////////////////////////

struct TStracerResult
    : public NYTree::TYsonSerializableLite
{
    yhash_map<int, TStrace> Traces;

    TStracerResult();
};

////////////////////////////////////////////////////////////////////////////////

TStracerResult Strace(const std::vector<int>& pids);

////////////////////////////////////////////////////////////////////////////////

struct TStraceTool
{
    TStracerResult operator()(const std::vector<int>& pids) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // NYT
