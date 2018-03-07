#pragma once

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TStrace
    : public NYTree::TYsonSerializable
{
    TString Trace;
    TString ProcessName;
    std::vector<TString> ProcessCommandLine;

    TStrace();
};

DEFINE_REFCOUNTED_TYPE(TStrace)

////////////////////////////////////////////////////////////////////////////////

struct TStracerResult
    : public NYTree::TYsonSerializable
{
    THashMap<int, TStracePtr> Traces;

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

} // namespace NYT
