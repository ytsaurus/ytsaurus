#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TStrace
    : public NYTree::TYsonSerializable
{
    Stroka Trace;
    Stroka ProcessName;
    std::vector<Stroka> ProcessCommandLine;

    TStrace();
};

DEFINE_REFCOUNTED_TYPE(TStrace)

////////////////////////////////////////////////////////////////////////////////

struct TStracerResult
    : public NYTree::TYsonSerializable
{
    yhash_map<int, TStracePtr> Traces;

    TStracerResult();
};

DEFINE_REFCOUNTED_TYPE(TStracerResult)

////////////////////////////////////////////////////////////////////////////////

TStracerResultPtr Strace(const std::vector<int>& pids);

////////////////////////////////////////////////////////////////////////////////

struct TStraceTool
{
    TStracerResultPtr operator()(const std::vector<int>& pids) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
