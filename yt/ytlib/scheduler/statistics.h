#pragma once

#include <ytlib/chunk_client/data_statistics.pb.h>

#include <core/ytree/forwarding_yson_consumer.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/convert.h>

#include <core/actions/callback.h>

#include <core/misc/property.h>
#include <core/misc/phoenix.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

template <class T>
class TBaseStatistics
{
protected:
    typedef yhash_map<NYPath::TYPath, T> TStatisticsMap;

public:
    T Get(const NYPath::TYPath& name) const;

    typename TStatisticsMap::const_iterator begin() const;
    typename TStatisticsMap::const_iterator end() const;

    void Persist(NPhoenix::TPersistenceContext& context);

protected:
    TStatisticsMap Data_;

    template <class U>
    friend void Serialize(const TBaseStatistics<U>& statistics, NYson::IYsonConsumer* consumer);
};

template <class T>
void Serialize(const TBaseStatistics<T>& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////

class TStatistics
    : public TBaseStatistics<i64>
{
public:
    void Add(const NYPath::TYPath& name, i64 value);

    template <class T>
    void AddComplex(const NYPath::TYPath& path, const T& statistics);

    template <class T>
    T GetComplex(const NYPath::TYPath& path) const;

    void AddSuffixToNames(const Stroka& suffix);

    void Merge(const TStatistics& other);

private:
    friend void Deserialize(TStatistics& value, NYTree::INodePtr node);

    friend class TAggregatedStatistics;
};

void Deserialize(TStatistics& value, NYTree::INodePtr node);

NChunkClient::NProto::TDataStatistics GetTotalInputDataStatistics(const TStatistics& statistics);
NChunkClient::NProto::TDataStatistics GetTotalOutputDataStatistics(const TStatistics& statistics);

extern const NYTree::TYsonString SerializedEmptyStatistics;

////////////////////////////////////////////////////////////////////

class TStatisticsConsumer
    : public NYTree::TForwardingYsonConsumer
{
public:
    typedef TCallback<void(const TStatistics&)> TParsedStatisticsConsumer;
    explicit TStatisticsConsumer(TParsedStatisticsConsumer consumer, const NYPath::TYPath& path);

private:
    NYPath::TYPath Path_;
    std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
    TParsedStatisticsConsumer Consumer_;

    virtual void OnMyListItem() override;
    void ProcessItem();

};

////////////////////////////////////////////////////////////////////

class TSummary
{
public:
    TSummary();

    void AddSample(i64 value);

    DEFINE_BYVAL_RO_PROPERTY(i64, Sum);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);
    DEFINE_BYVAL_RO_PROPERTY(i64, Min);
    DEFINE_BYVAL_RO_PROPERTY(i64, Max);

    void Persist(NPhoenix::TPersistenceContext& context);

    friend void Deserialize(TSummary& value, NYTree::INodePtr node);

};

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer);
void Deserialize(TSummary& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////

class TAggregatedStatistics
    : public TBaseStatistics<TSummary>
{
public:
    void AddSample(const TStatistics& statistics);
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define STATISTICS_INL_H_
#include "statistics-inl.h"
#undef STATISTICS_INL_H_
