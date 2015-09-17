#pragma once

#include <ytlib/job_tracker_client/job.pb.h>

#include <ytlib/chunk_client/data_statistics.pb.h>

#include <core/ytree/forwarding_yson_consumer.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/convert.h>

#include <core/actions/callback.h>

#include <core/misc/property.h>
#include <core/misc/phoenix.h>

namespace NYT {
namespace NJobTrackerClient {

////////////////////////////////////////////////////////////////////

class TSummary
{
public:
    TSummary();

    void AddSample(i64 sample);

    void Update(const TSummary& summary);

    void Reset();

    DEFINE_BYVAL_RO_PROPERTY(i64, Sum);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);
    DEFINE_BYVAL_RO_PROPERTY(i64, Min);
    DEFINE_BYVAL_RO_PROPERTY(i64, Max);

    void Persist(NPhoenix::TPersistenceContext& context);

    friend void FromProto(TSummary* summary, const NProto::TStatistics::TSummary& protoSummary);
};

void ToProto(NProto::TStatistics::TSummary* protoSummary, const TSummary& summary);
void FromProto(TSummary* summary, const NProto::TStatistics::TSummary& protoSummary);

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////

class TStatistics
{
    using TSummaryMap = std::map<NYPath::TYPath, TSummary>;
    DEFINE_BYREF_RO_PROPERTY(TSummaryMap, Data);

public:
    void AddSample(const NYPath::TYPath& path, i64 sample);

    void AddSample(const NYPath::TYPath& path, const NYTree::INodePtr& sample);

    template <class T>
    void AddSample(const NYPath::TYPath& path, const T& sample);

    void Update(const TStatistics& statistics);

    void AddSuffixToNames(const Stroka& suffix);

    void Persist(NPhoenix::TPersistenceContext& context);

private:
    TSummary& GetSummary(const NYPath::TYPath& path);

    friend void FromProto(TStatistics* statistics, const NProto::TStatistics& protoStatistics);
};

template <class T>
T GetValues(
    const TStatistics& statistics, 
    const NYPath::TYPath& path, 
    std::function<i64(const TSummary&)> getValue);

void ToProto(NProto::TStatistics* protoStatistics, const TStatistics& statistics);
void FromProto(TStatistics* statistics, const NProto::TStatistics& protoStatistics);

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////

NChunkClient::NProto::TDataStatistics GetTotalInputDataStatistics(const TStatistics& statistics);
NChunkClient::NProto::TDataStatistics GetTotalOutputDataStatistics(const TStatistics& statistics);

////////////////////////////////////////////////////////////////////

class TStatisticsConsumer
    : public NYTree::TForwardingYsonConsumer
{
public:
    typedef TCallback<void(const NYTree::INodePtr& sample)> TSampleHandler;
    explicit TStatisticsConsumer(TSampleHandler consumer);

private:
    std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
    TSampleHandler SampleHandler_;

    virtual void OnMyListItem() override;

    void ProcessSample();
};

////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT

#define STATISTICS_INL_H_
#include "statistics-inl.h"
#undef STATISTICS_INL_H_
