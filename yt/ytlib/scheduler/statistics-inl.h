#ifndef STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include statistics.h"
#endif
#undef STATISTICS_INL_H_

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TStatistics::Add(const NYPath::TYPath& path, const T& statistics)
{
    TStatisticsConsumer consumer(
        BIND([=] (const TStatistics& other) {
            Merge(other);
        }),
        path);

    Serialize(statistics, &consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
