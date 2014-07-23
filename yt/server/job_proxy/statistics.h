#pragma once

#include <core/misc/property.h>
#include <core/yson/consumer.h>
#include <core/ytree/public.h>
#include <core/actions/bind.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TSummary
{
public:
    TSummary();
    explicit TSummary(i64 value);
    void Merge(const TSummary& other);

    DEFINE_BYVAL_RO_PROPERTY(i64, Summ);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);
    DEFINE_BYVAL_RO_PROPERTY(i64, Min);
    DEFINE_BYVAL_RO_PROPERTY(i64, Max);

    friend void Deserialize(TSummary& value, NYTree::INodePtr node);
};

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer);
void Deserialize(TSummary& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////

class TStatistics
{
public:
    void Add(const Stroka& name, const TSummary& summary);
    void Merge(const TStatistics& other);
    void Clear();
    bool Empty() const;

    typedef std::map<Stroka, TSummary> TSummaryDict;
    DEFINE_BYREF_RO_PROPERTY(TSummaryDict, Statistics);

    friend void Deserialize(TStatistics& value, NYTree::INodePtr node);
};

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer);
void Deserialize(TStatistics& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////

class TStatisticsConvertor
    : public NYson::TYsonConsumerBase
{
public:
    typedef TCallback<void(const TStatistics&)> TStatisticsConsumer;
    explicit TStatisticsConvertor(TStatisticsConsumer consumer);

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnIntegerScalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnEntity() override;

    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;

    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

private:
    int Depth_;
    Stroka LastKey_;
    TStatistics Statistics_;

    TStatisticsConsumer Consumer_;
};

////////////////////////////////////////////////////////////////////

} // NJobProxy
} // NYT
