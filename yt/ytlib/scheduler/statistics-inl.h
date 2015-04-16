#ifndef STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include statistics.h"
#endif
#undef STATISTICS_INL_H_

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TBaseStatistics<T>::Get(const NYPath::TYPath& name) const
{
    auto it = Data_.find(name);
    if (it != Data_.end()) {
        return it->second;
    }
    THROW_ERROR_EXCEPTION("No such statistic %Qv", name);
}

template <class T>
void Serialize(const TBaseStatistics<T>& statistics, NYson::IYsonConsumer* consumer)
{
    auto root = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (const auto& pair : statistics.Data_) {
        ForceYPath(root, pair.first);
        auto value = NYTree::ConvertToNode(pair.second);
        SetNodeByYPath(root, pair.first, value);
    }
    Serialize(*root, consumer);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TStatistics::AddComplex(const NYPath::TYPath& path, const T& statistics)
{
    TStatisticsConsumer consumer(
        BIND([=] (const TStatistics& other) {
            Merge(other);
        }),
        path);
    consumer.OnListItem();

    Serialize(statistics, &consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
