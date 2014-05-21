#include "stdafx.h"
#include "statistics.h"

#include <core/ytree/fluent.h>
#include <ytlib/chunk_client/data_statistics.h>
#include <ytlib/cgroup/statistics.h>

namespace NYT {
namespace NJobTrackerClient {

namespace NProto {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////

TJobStatistics& operator+= (TJobStatistics& lhs, const TJobStatistics& rhs)
{
    *lhs.mutable_input() += rhs.input();
    *lhs.mutable_output() += rhs.output();
    lhs.set_time(lhs.time() + rhs.time());
    *lhs.mutable_cpu() += rhs.cpu();
    *lhs.mutable_block_io() += rhs.block_io();
    return lhs;
}

TJobStatistics operator+ (const TJobStatistics& lhs, const TJobStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TJobStatistics& operator-= (TJobStatistics& lhs, const TJobStatistics& rhs)
{
    *lhs.mutable_input() -= rhs.input();
    *lhs.mutable_output() -= rhs.output();
    lhs.set_time(lhs.time() - rhs.time());
    *lhs.mutable_cpu() -= rhs.cpu();
    *lhs.mutable_block_io() -= rhs.block_io();
    return lhs;
}

TJobStatistics operator- (const TJobStatistics& lhs, const TJobStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TJobStatistics GetZeroJobStatistics()
{
    TJobStatistics statistics;
    *statistics.mutable_input() = NChunkClient::NProto::ZeroDataStatistics();
    *statistics.mutable_output() = NChunkClient::NProto::ZeroDataStatistics();
    statistics.set_time(0);
    return statistics;
}

const TJobStatistics& ZeroJobStatistics()
{
    static const TJobStatistics statistics = GetZeroJobStatistics();
    return statistics;
}

void Serialize(const TJobStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("input").Value(statistics.input())
            .Item("output").Value(statistics.output())
            .Item("time").Value(statistics.time())
            .DoIf(statistics.has_cpu(), [&] (TFluentMap fluent) {
                fluent.Item("cpu").Value(statistics.cpu());
            })
            .DoIf(statistics.has_block_io(), [&] (TFluentMap fluent) {
                fluent.Item("block_io").Value(statistics.block_io());
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NJobTrackerClient
} // namespace NYT
