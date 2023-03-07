#include "statistics_producer.h"

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/stack_trace.h>

namespace NYT::NYTAlloc {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateStatisticsProducer()
{
    return BIND([] (IYsonConsumer* consumer) {
        auto statistics = NYTAlloc::GetProfiledAllocationStatistics();
        std::sort(
            statistics.begin(),
            statistics.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.Counters[EBasicCounter::BytesUsed] > rhs.Counters[EBasicCounter::BytesUsed];
            });
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("profiled_allocations").DoListFor(statistics, [] (auto fluent, auto& allocation) {
                    fluent
                        .Item().BeginMap()
                            .DoIf(allocation.Backtrace.FrameCount > 0, [&] (auto fluent) {
                                fluent
                                    .Item("backtrace").Do([&] (auto fluent) {
                                        fluent.GetConsumer()->OnBeginList();
                                        FormatStackTrace(
                                            const_cast<void**>(allocation.Backtrace.Frames.data()), allocation.Backtrace.FrameCount,
                                            [&] (const char* data, size_t length) {
                                                fluent.GetConsumer()->OnListItem();
                                                auto line = TStringBuf(data, length);
                                                if (line.EndsWith('\n')) {
                                                    line = line.Trunc(line.length() - 1);
                                                }
                                                fluent.GetConsumer()->OnStringScalar(line);
                                            });
                                        fluent.GetConsumer()->OnEndList();
                                    });
                            })
                            .Item("counters").Value(allocation.Counters)
                        .EndMap();
                })
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
