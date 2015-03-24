#pragma once

#include <core/yson/public.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TStracerResult
{
    yhash_map<int, TStrace> Traces;
};


void Serialize(const TStracerResult& stracerResult, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

TStracerResult Strace(const std::vector<int>& pids);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // NYT