#include "stdafx.h"
#include "statistics.h"

#include <core/ytree/fluent.h>
#include <ytlib/chunk_client/data_statistics.h>

namespace NYT {
namespace NJobTrackerClient {

namespace NProto {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////

NProto::TJobStatistics& operator+= (NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs)
{
	*lhs.mutable_input() = lhs.input() + rhs.input();
    *lhs.mutable_output() = lhs.output() + rhs.output();
    lhs.set_time(lhs.time() + rhs.time());
    return lhs;
}

NProto::TJobStatistics operator+ (const NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

NProto::TJobStatistics& operator-= (NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs)
{
    *lhs.mutable_input() = lhs.input() - rhs.input();
    *lhs.mutable_output() = lhs.output() - rhs.output();
    lhs.set_time(lhs.time() - rhs.time());
    return lhs;
}

NProto::TJobStatistics operator- (const NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

NProto::TJobStatistics GetZeroJobStatistics()
{
    NProto::TJobStatistics statistics;
    *statistics.mutable_input() = NChunkClient::NProto::ZeroDataStatistics();
    *statistics.mutable_output() = NChunkClient::NProto::ZeroDataStatistics();
    statistics.set_time(0);
    return statistics;
}

const NProto::TJobStatistics& ZeroJobStatistics()
{
    static const NProto::TJobStatistics statistics = GetZeroJobStatistics();
    return statistics;
}

void Serialize(const NProto::TJobStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("input").Value(statistics.input())
            .Item("output").Value(statistics.output())
            .Item("time").Value(statistics.time())
        .EndMap();
}

////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NJobTrackerClient
} // namespace NYT
