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

TJobStatistics& operator+= (TJobStatistics& lhs, const TJobStatistics& rhs)
{
    *lhs.mutable_input() += rhs.input();

    for (auto i = 0; i < rhs.output_size(); ++i) {
        if (i >= lhs.output_size()) {
            YCHECK(i == lhs.output_size());
            *lhs.add_output() = NChunkClient::NProto::ZeroDataStatistics();
        }
        YCHECK(i < lhs.output_size());
        *lhs.mutable_output(i) += rhs.output(i);
    }

    lhs.set_time(lhs.time() + rhs.time());

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

    for (auto i = 0; i < rhs.output_size(); ++i) {
        if (i >= lhs.output_size()) {
            YCHECK(i == lhs.output_size());
            *lhs.add_output() = NChunkClient::NProto::ZeroDataStatistics();
        }
        YCHECK(i < lhs.output_size());
        *lhs.mutable_output(i) -= rhs.output(i);
    }

    lhs.set_time(lhs.time() - rhs.time());

    return lhs;
}

TJobStatistics operator- (const TJobStatistics& lhs, const TJobStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

NChunkClient::NProto::TDataStatistics GetTotalOutput(const TJobStatistics& statistics)
{
    auto result = NChunkClient::NProto::ZeroDataStatistics();
    for (auto i = 0; i < statistics.output_size(); ++i) {
        result += statistics.output(i);
    }
    return result;
}

TJobStatistics GetZeroJobStatistics()
{
    TJobStatistics statistics;
    *statistics.mutable_input() = NChunkClient::NProto::ZeroDataStatistics();
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
            .Item("output").DoListFor(0, statistics.output_size(), [&statistics] (TFluentList list, int index) {
                list.Item().Value(statistics.output(index));
            })
            .Item("time").Value(statistics.time())
        .EndMap();
}

////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NJobTrackerClient
} // namespace NYT
