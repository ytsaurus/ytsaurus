#pragma once

#include <core/yson/public.h>

#include <core/ytree/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TStrace
{
    Stroka Trace;
    Stroka ProcessName;
    std::vector<Stroka> ProcessCommandLine;
};

void Serialize(const TStrace& trace, NYson::IYsonConsumer* consumer);
void Deserialize(TStrace& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TStracerResult
{
    yhash_map<int, TStrace> Traces;
};

void Serialize(const TStracerResult& stracerResult, NYson::IYsonConsumer* consumer);
void Deserialize(TStracerResult& value, NYTree::INodePtr node);

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