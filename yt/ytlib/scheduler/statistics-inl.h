#ifndef STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include statistics.h"
#endif

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
typename TBaseStatistics<T>::TStatisticsMap::const_iterator TBaseStatistics<T>::begin() const
{
    return Data_.begin();
}

template <class T>
typename TBaseStatistics<T>::TStatisticsMap::const_iterator TBaseStatistics<T>::end() const
{
    return Data_.end();
}

template <class T>
void TBaseStatistics<T>::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Data_);
}

template <class T>
void Serialize(const TBaseStatistics<T>& statistics, NYson::IYsonConsumer* consumer)
{
    auto root = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (const auto& pair : statistics.Data_) {
        ForceYPath(root, pair.first);
        auto value = NYTree::ConvertToNode(pair.second);
        SetNodeByYPath(root, pair.first, std::move(value));
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

template <class T>
T TStatistics::GetComplex(const NYPath::TYPath& path) const
{
    auto root = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (const auto& pair : Data_) {
        if (path.is_prefix(pair.first)) {
            auto subPath = pair.first.substr(path.size());

            ForceYPath(root, subPath);
            auto value = NYTree::ConvertToNode(pair.second);
            SetNodeByYPath(root, subPath, std::move(value));
        }
    }

    T result;
    Deserialize(result, std::move(root));
    return result;
 }


////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
