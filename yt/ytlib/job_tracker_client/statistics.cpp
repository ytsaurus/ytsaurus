#include "stdafx.h"
#include "statistics.h"

#include <ytlib/chunk_client/data_statistics.h>

#include <core/ytree/fluent.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NJobTrackerClient {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NPhoenix;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////

TSummary::TSummary()
{ 
    Reset();
}

void TSummary::AddSample(i64 sample)
{
    Sum_ += sample;
    Count_ += 1;
    Min_ = std::min(Min_, sample);
    Max_ = std::max(Max_, sample);
}

void TSummary::Update(const TSummary& summary)
{
    Sum_ += summary.GetSum();
    Count_ += summary.GetCount();
    Min_ = std::min(Min_, summary.GetMin());
    Max_ = std::max(Max_, summary.GetMax());
}

void TSummary::Reset()
{
    Sum_ = 0;
    Count_ = 0;
    Min_ = std::numeric_limits<i64>::max();
    Max_ = std::numeric_limits<i64>::min();
}

void TSummary::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Sum_);
    Persist(context, Count_);
    Persist(context, Min_);
    Persist(context, Max_);
}

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("sum").Value(summary.GetSum())
            .Item("count").Value(summary.GetCount())
            .Item("min").Value(summary.GetMin())
            .Item("max").Value(summary.GetMax())
        .EndMap();
}

void ToProto(NProto::TStatistics::TSummary* protoSummary, const TSummary& summary)
{
    protoSummary->set_sum(summary.GetSum());
    protoSummary->set_min(summary.GetMin());
    protoSummary->set_max(summary.GetMax());
    protoSummary->set_count(summary.GetCount());
}

void FromProto(TSummary* summary, const NProto::TStatistics::TSummary& protoSummary)
{
    summary->Sum_ = protoSummary.sum();
    summary->Min_ = protoSummary.min();
    summary->Max_ = protoSummary.max();
    summary->Count_ = protoSummary.count();
}

////////////////////////////////////////////////////////////////////

TSummary& TStatistics::GetSummary(const NYPath::TYPath& path)
{
    auto result = Data_.insert(std::make_pair(path, TSummary()));
    auto it = result.first;
    if (result.second) {
        // This is a new statistic, check validity.
        if (it != Data_.begin()) {
            auto prev = std::prev(it);
            if (HasPrefix(it->first, prev->first)) {
                THROW_ERROR_EXCEPTION(
                    "Incompatible statistic paths (OldPath: %v, NewPath: %v)",
                    prev->first,
                    it->first);
            }
        }
        auto next = std::next(it);
        if (next != Data_.end()) {
            if (HasPrefix(next->first, it->first)) {
                THROW_ERROR_EXCEPTION(
                    "Incompatible statistic paths (OldPath: %v, NewPath: %v)",
                    next->first,
                    it->first);
            }
        }
    }

    return it->second;
}

void TStatistics::AddSample(const NYPath::TYPath& path, i64 sample)
{
    GetSummary(path).AddSample(sample);
}

void TStatistics::AddSample(const NYPath::TYPath& path, const INodePtr& sample)
{
    switch (sample->GetType()) {
        case ENodeType::Int64:
            AddSample(path, sample->AsInt64()->GetValue());
            break;

        case ENodeType::Uint64:
            AddSample(path, static_cast<i64>(sample->AsUint64()->GetValue()));
            break;

        case ENodeType::Map:
            for (auto& pair : sample->AsMap()->GetChildren()) {
                AddSample(path + "/" + pair.first, pair.second);
            }
            break;

        default:
            THROW_ERROR_EXCEPTION(
                "Invalid statistics type: expected map or integral type (Value: %v)", 
                ConvertToYsonString(sample, EYsonFormat::Text).Data());
    }
}

void TStatistics::Update(const TStatistics& statistics)
{
    for (const auto& pair : statistics.Data()) {
        GetSummary(pair.first).Update(pair.second);
    }
}

void TStatistics::AddSuffixToNames(const Stroka& suffix)
{
    TSummaryMap newData;
    for (const auto& pair : Data_) {
        newData[pair.first + suffix] = pair.second;
    }

    std::swap(Data_, newData);
}

void TStatistics::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Data_);
}

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    auto root = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& pair : statistics.Data()) {
        ForceYPath(root, pair.first);
        auto value = ConvertToNode(pair.second);
        SetNodeByYPath(root, pair.first, std::move(value));
    }

    Serialize(*root, consumer);
}

void ToProto(NProto::TStatistics* protoStatistics, const TStatistics& statistics)
{
    for (const auto& pair : statistics.Data()) {
        auto* protoSummary = protoStatistics->add_summaries();
        protoSummary->set_path(pair.first);
        ToProto(protoSummary, pair.second);
    }
}

void FromProto(TStatistics* statistics, const NProto::TStatistics& protoStatistics)
{
    using NYT::FromProto;
    for (const auto& protoSummary : protoStatistics.summaries()) {
        auto summary = FromProto<TSummary>(protoSummary);
        YCHECK(statistics->Data_.insert(std::make_pair(
            protoSummary.path(), 
            summary)).second);
    }
}

////////////////////////////////////////////////////////////////////

TDataStatistics GetTotalInputDataStatistics(const TStatistics& jobStatistics)
{
    auto getValue = [] (const TSummary& summary) {
        return summary.GetSum();
    };

    try {
        return GetValues<TDataStatistics>(jobStatistics, "/data/input", getValue);
    } catch (const std::exception&) {
        return ZeroDataStatistics();
    }
}

yhash_map<int, TDataStatistics> GetOutputDataStatistics(const TStatistics& jobStatistics)
{
    auto getValue = [] (const TSummary& summary) {
        return summary.GetSum();
    };

    try {
        return GetValues<yhash_map<int, TDataStatistics>>(jobStatistics, "/data/output", getValue);
    } catch (const std::exception&) {
        return yhash_map<int, TDataStatistics>();
    }
}

TDataStatistics GetTotalOutputDataStatistics(const TStatistics& jobStatistics)
{
    TDataStatistics result = ZeroDataStatistics();
    for (const auto& pair : GetOutputDataStatistics(jobStatistics)) {
        result += pair.second;
    }
    return result;
}

////////////////////////////////////////////////////////////////////

TStatisticsConsumer::TStatisticsConsumer(
    TSampleHandler sampleHandler)
    : TreeBuilder_(CreateBuilderFromFactory(GetEphemeralNodeFactory()))
    , SampleHandler_(sampleHandler)
{ }

void TStatisticsConsumer::OnMyListItem()
{
    TreeBuilder_->BeginTree();
    Forward(TreeBuilder_.get(), BIND(&TStatisticsConsumer::ProcessSample, this), NYson::EYsonType::Node);
}

void TStatisticsConsumer::ProcessSample()
{
    auto node = TreeBuilder_->EndTree();
    SampleHandler_.Run(node);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
