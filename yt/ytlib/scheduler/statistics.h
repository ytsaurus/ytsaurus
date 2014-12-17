#pragma once

#include <core/misc/property.h>

#include <core/yson/consumer.h>

// should be removed
// TODO(babenko): so let's remove it
#include <core/ytree/tree_builder.h>
#include <core/ytree/public.h>

#include <core/actions/bind.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TSummary
{
public:
    TSummary();
    explicit TSummary(i64 value);

    void Merge(const TSummary& other);

    DEFINE_BYVAL_RO_PROPERTY(i64, Sum);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);
    DEFINE_BYVAL_RO_PROPERTY(i64, Min);
    DEFINE_BYVAL_RO_PROPERTY(i64, Max);

    // TODO(babenko): below we also declare Serialize to be a friend; is it really needed?
    friend void Deserialize(TSummary& value, NYTree::INodePtr node);
};

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer);
void Deserialize(TSummary& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////

class TStatistics
{
public:
    void Add(const NYPath::TYPath& name, const TSummary& summary);
    void Merge(const TStatistics& other);
    void Clear();
    bool IsEmpty() const;

    TSummary Get(const NYPath::TYPath& name) const;

private:
    // TODO(babenko): PathToSummary_?
    yhash_map<NYPath::TYPath, TSummary> PathToSummary_;

    friend void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TStatistics& value, NYTree::INodePtr node);
};

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer);
void Deserialize(TStatistics& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////

class TStatisticsConsumer
    : public NYson::TYsonConsumerBase
{
public:
    typedef TCallback<void(const TStatistics&)> TParsedStatisticsConsumer;
    explicit TStatisticsConsumer(TParsedStatisticsConsumer consumer, const NYPath::TYPath& path);

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
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
    NYPath::TYPath Path_;
    std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
    TParsedStatisticsConsumer Consumer_;

    void ConvertToStatistics(TStatistics& value, NYTree::INodePtr node);
};

////////////////////////////////////////////////////////////////////

// TODO(babenko): move to inl
// why isn't it a member?
template <typename T>
void AddStatistic(TStatistics& customStatistics, const NYPath::TYPath& path, const T& statistics)
{
    auto consume = [&customStatistics] (const TStatistics& other) {
        customStatistics.Merge(other);
    };

    TStatisticsConsumer consumer(BIND(consume), path);
    Serialize(statistics, &consumer);
}

// TODO(babenko): need a separator here

} // namespace NScheduler
} // namespace NYT
