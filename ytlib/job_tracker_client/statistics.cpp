#include "statistics.h"

#include <yt/ytlib/chunk_client/data_statistics.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/fluent.h>

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

TSummary::TSummary(i64 sum, i64 count, i64 min, i64 max)
    : Sum_(sum)
    , Count_(count)
    , Min_(min)
    , Max_(max)
{ }

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

bool TSummary::operator ==(const TSummary& other) const
{
    return 
        Sum_ == other.Sum_ && 
        Count_ == other.Count_ && 
        Min_ == other.Min_ &&
        Max_ == other.Max_;
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
                Data_.erase(it);
                THROW_ERROR_EXCEPTION(
                    "Incompatible statistic paths: old %v, new %v",
                    prev->first,
                    path);
            }
        }
        auto next = std::next(it);
        if (next != Data_.end()) {
            if (HasPrefix(next->first, it->first)) {
                Data_.erase(it);
                THROW_ERROR_EXCEPTION(
                    "Incompatible statistic paths: old %v, new %v",
                    next->first,
                    path);
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
                "Invalid statistics type: expected map or integral type but found %v",
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

////////////////////////////////////////////////////////////////////

class TStatisticsBuildingConsumer
    : public TYsonConsumerBase
    , public IBuildingYsonConsumer<TStatistics>
{
public:
    virtual void OnStringScalar(const TStringBuf& value) override
    {
        YUNREACHABLE();
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        AtSummaryMap_ = true;
        if (LastKey_ == "sum") {
            CurrentSummary_.Sum_ = value;
        } else if (LastKey_ == "count") {
            CurrentSummary_.Count_ = value;
        } else if (LastKey_ == "min") {
            CurrentSummary_.Min_ = value;
        } else if (LastKey_ == "max") {
            CurrentSummary_.Max_ = value;
        } else {
            YUNREACHABLE();
        }
        ++FilledSummaryFields_;
    }
    
    virtual void OnUint64Scalar(ui64 value) override
    {
        YUNREACHABLE();
    }
    
    virtual void OnDoubleScalar(double value) override
    {
        YUNREACHABLE();
    }
    
    virtual void OnBooleanScalar(bool value) override
    {
        YUNREACHABLE();
    }
    
    virtual void OnEntity() override
    {
        YUNREACHABLE();
    }
    
    virtual void OnBeginList() override
    {
        YUNREACHABLE();
    }
    
    virtual void OnListItem() override
    {
        YUNREACHABLE();
    }

    virtual void OnEndList() override
    {
        YUNREACHABLE();
    }

    virtual void OnBeginMap() override
    {
        // If we are here, we are either:
        // * at the root (then do nothing)
        // * at some directory (then the last key was the directory name)
        if (!LastKey_.empty()) {
            DirectoryNameLengths_.push_back(LastKey_.size());
            CurrentPath_.append('/');
            CurrentPath_.append(LastKey_);
            LastKey_.clear();
        } else {
            YCHECK(CurrentPath_.empty());
        }
    }  

    virtual void OnKeyedItem(const TStringBuf& key) override
    {
        LastKey_ = key;
    }

    virtual void OnEndMap() override
    { 
        if (AtSummaryMap_) {
            YCHECK(FilledSummaryFields_ == 4);
            Statistics_.Data_[CurrentPath_] = CurrentSummary_;
            FilledSummaryFields_ = 0;
            AtSummaryMap_ = false;
        }
        
        if (!CurrentPath_.empty()) {
            // We need to go to the parent.
            CurrentPath_.resize(CurrentPath_.size() - DirectoryNameLengths_.back() - 1);
            DirectoryNameLengths_.pop_back();
        }
    }
    
    virtual void OnBeginAttributes() override
    {
        YUNREACHABLE();
    }

    virtual void OnEndAttributes() override
    {
        YUNREACHABLE();
    }

    virtual TStatistics Finish() override
    {
        return Statistics_;
    }

private:
    TStatistics Statistics_;
    
    Stroka CurrentPath_;
    std::vector<int> DirectoryNameLengths_;

    TSummary CurrentSummary_;
    i64 FilledSummaryFields_ = 0;

    Stroka LastKey_;

    bool AtSummaryMap_ = false;

};

void CreateBuildingYsonConsumer(std::unique_ptr<IBuildingYsonConsumer<TStatistics>>* buildingConsumer, EYsonType ysonType)
{
    YCHECK(ysonType == EYsonType::Node);
    *buildingConsumer = std::make_unique<TStatisticsBuildingConsumer>();
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
        return TDataStatistics();
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
    TDataStatistics result;
    for (const auto& pair : GetOutputDataStatistics(jobStatistics)) {
        result += pair.second;
    }
    return result;
}

////////////////////////////////////////////////////////////////////

TStatisticsConsumer::TStatisticsConsumer(TSampleHandler sampleHandler)
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
