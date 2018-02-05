#include "statistics.h"

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ypath/token.h>
#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/serialize.h>

#include <util/string/util.h>

namespace NYT {
namespace NJobTrackerClient {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NPhoenix;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

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
                if (pair.first.Empty()) {
                    THROW_ERROR_EXCEPTION("Statistic cannot have an empty name")
                        << TErrorAttribute("path_prefix", path);
                }
                AddSample(path + "/" + ToYPathLiteral(pair.first), pair.second);
            }
            break;

        default:
            THROW_ERROR_EXCEPTION(
                "Invalid statistics type: expected map or integral type but found %v",
                ConvertToYsonString(sample, EYsonFormat::Text).GetData());
    }
}

void TStatistics::Update(const TStatistics& statistics)
{
    for (const auto& pair : statistics.Data()) {
        GetSummary(pair.first).Update(pair.second);
    }
}

void TStatistics::AddSuffixToNames(const TString& suffix)
{
    TSummaryMap newData;
    for (const auto& pair : Data_) {
        newData[pair.first + suffix] = pair.second;
    }

    std::swap(Data_, newData);
}

TStatistics::TSummaryRange TStatistics::GetRangeByPrefix(const TString& prefix) const
{
    auto begin = Data().lower_bound(prefix + '/');
    // This will efficiently return an iterator to the first path not starting with "`prefix`/".
    auto end = Data().lower_bound(prefix + ('/' + 1));
    return TSummaryRange(begin, end);
}

void TStatistics::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Data_);
}

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    using NYT::Serialize;

    if (statistics.GetTimestamp()) {
        consumer->OnBeginAttributes();
        consumer->OnKeyedItem("timestamp");
        NYTree::Serialize(*statistics.GetTimestamp(), consumer);
        consumer->OnEndAttributes();
    }
    consumer->OnBeginMap();

    // Depth of the previous key defined as a number of nested maps before the summary itself.
    int previousDepth = 0;
    TYPath previousKey;
    for (const auto& pair : statistics.Data()) {
        const auto& currentKey = pair.first;
        NYPath::TTokenizer previousTokenizer(previousKey);
        NYPath::TTokenizer currentTokenizer(currentKey);

        // The depth of the common part of two keys, needed to determine the number of maps to close.
        int commonDepth = 0;

        if (previousKey) {
            // First we find the position in which current key is different from the
            // previous one in order to close necessary number of maps.
            commonDepth = 0;
            while (true)
            {
                currentTokenizer.Advance();
                previousTokenizer.Advance();
                // Note that both tokenizers can't reach EndOfStream token, because it would mean that
                // currentKey is prefix of prefixKey or vice versa that is prohibited in TStatistics.
                currentTokenizer.Expect(NYPath::ETokenType::Slash);
                previousTokenizer.Expect(NYPath::ETokenType::Slash);

                currentTokenizer.Advance();
                previousTokenizer.Advance();
                currentTokenizer.Expect(NYPath::ETokenType::Literal);
                previousTokenizer.Expect(NYPath::ETokenType::Literal);
                if (currentTokenizer.GetLiteralValue() == previousTokenizer.GetLiteralValue()) {
                    ++commonDepth;
                } else {
                    break;
                }
            }
            // Close all redundant maps.
            while (previousDepth > commonDepth) {
                consumer->OnEndMap();
                --previousDepth;
            }
        } else {
            currentTokenizer.Advance();
            currentTokenizer.Expect(NYPath::ETokenType::Slash);
            currentTokenizer.Advance();
            currentTokenizer.Expect(NYPath::ETokenType::Literal);
        }

        int currentDepth = commonDepth;
        // Open all newly appeared maps.
        while (true) {
            consumer->OnKeyedItem(currentTokenizer.GetLiteralValue());
            currentTokenizer.Advance();
            if (currentTokenizer.GetType() == NYPath::ETokenType::Slash) {
                consumer->OnBeginMap();
                ++currentDepth;
                currentTokenizer.Advance();
                currentTokenizer.Expect(NYPath::ETokenType::Literal);
            } else if (currentTokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                break;
            } else {
                YCHECK(false && "Wrong token type in statistics key");
            }
        }
        // Serialize summary.
        Serialize(pair.second, consumer);

        previousDepth = currentDepth;
        previousKey = currentKey;
    }
    while (previousDepth > 0) {
        consumer->OnEndMap();
        --previousDepth;
    }

    // This OnEndMap is complementary to the OnBeginMap before the main loop.
    consumer->OnEndMap();
}

// Helper function for GetNumericValue.
i64 GetSum(const TSummary& summary)
{
    return summary.GetSum();
}

i64 GetNumericValue(const TStatistics& statistics, const TString& path)
{
    auto value = FindNumericValue(statistics, path);
    if (!value) {
        THROW_ERROR_EXCEPTION("Statistics %v is not present",
            path);
    } else {
        return *value;
    }
}

TNullable<i64> FindNumericValue(const TStatistics& statistics, const TString& path)
{
    auto summary = FindSummary(statistics, path);
    return summary ? MakeNullable(summary->GetSum()) : Null;
}

TNullable<TSummary> FindSummary(const TStatistics& statistics, const TString& path)
{
    const auto& data = statistics.Data();
    auto iterator = data.lower_bound(path);
    if (iterator != data.end() && iterator->first != path && HasPrefix(iterator->first, path)) {
        THROW_ERROR_EXCEPTION("Invalid statistics type: cannot get summary of %v since it is a map",
            path);
    } else if (iterator == data.end() || iterator->first != path) {
        return Null;
    } else {
        return iterator->second;
    }
}


////////////////////////////////////////////////////////////////////////////////

class TStatisticsBuildingConsumer
    : public TYsonConsumerBase
    , public IBuildingYsonConsumer<TStatistics>
{
public:
    virtual void OnStringScalar(const TStringBuf& value) override
    {
        if (!AtAttributes_) {
            THROW_ERROR_EXCEPTION("String scalars are not allowed for statistics");
        }
        Statistics_.SetTimestamp(ConvertTo<TInstant>(value));
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        if (AtAttributes_) {
            THROW_ERROR_EXCEPTION("Timestamp should have string type");
        }
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
            THROW_ERROR_EXCEPTION("Invalid summary key for statistics")
                << TErrorAttribute("key", LastKey_);
        }
        ++FilledSummaryFields_;
    }

    virtual void OnUint64Scalar(ui64 value) override
    {
        THROW_ERROR_EXCEPTION("Uint64 scalars are not allowed for statistics");
    }

    virtual void OnDoubleScalar(double value) override
    {
        THROW_ERROR_EXCEPTION("Double scalars are not allowed for statistics");
    }

    virtual void OnBooleanScalar(bool value) override
    {
        THROW_ERROR_EXCEPTION("Boolean scalars are not allowed for statistics");
    }

    virtual void OnEntity() override
    {
        THROW_ERROR_EXCEPTION("Entities are not allowed for statistics");
    }

    virtual void OnBeginList() override
    {
        THROW_ERROR_EXCEPTION("Lists are not allowed for statistics");
    }

    virtual void OnListItem() override
    {
        THROW_ERROR_EXCEPTION("Lists are not allowed for statistics");
    }

    virtual void OnEndList() override
    {
        THROW_ERROR_EXCEPTION("Lists are not allowed for statistics");
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
            if (!CurrentPath_.empty()) {
                THROW_ERROR_EXCEPTION("Empty keys are not allowed for statistics");
            }
        }
    }

    virtual void OnKeyedItem(const TStringBuf& key) override
    {
        if (AtAttributes_) {
            if (key != "timestamp") {
                THROW_ERROR_EXCEPTION("Attributes other than \"timestamp\" are not allowed");
            }
        } else {
            LastKey_ = ToYPathLiteral(key);
        }
    }

    virtual void OnEndMap() override
    {
        if (AtSummaryMap_) {
            if (FilledSummaryFields_ != 4) {
                THROW_ERROR_EXCEPTION("All four summary fields should be filled for statistics");
            }
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
        if (!CurrentPath_.empty()) {
            THROW_ERROR_EXCEPTION("Attributes are not allowed for statistics");
        }
        AtAttributes_ = true;
    }

    virtual void OnEndAttributes() override
    {
        AtAttributes_ = false;
    }

    virtual TStatistics Finish() override
    {
        return Statistics_;
    }

private:
    TStatistics Statistics_;

    TString CurrentPath_;
    std::vector<int> DirectoryNameLengths_;

    TSummary CurrentSummary_;
    i64 FilledSummaryFields_ = 0;

    TString LastKey_;

    bool AtSummaryMap_ = false;
    bool AtAttributes_ = false;
};

void CreateBuildingYsonConsumer(std::unique_ptr<IBuildingYsonConsumer<TStatistics>>* buildingConsumer, EYsonType ysonType)
{
    YCHECK(ysonType == EYsonType::Node);
    *buildingConsumer = std::make_unique<TStatisticsBuildingConsumer>();
}

////////////////////////////////////////////////////////////////////////////////

const TString InputPrefix = "/data/input";
const TString OutputPrefix = "/data/output";
const TString OutputPipePrefix = "/user_job/pipes/output";
const TString IdleTimeSuffix = "/idle_time";

TDataStatistics GetTotalInputDataStatistics(const TStatistics& jobStatistics)
{
    TDataStatistics result;
    for (const auto& pair : jobStatistics.GetRangeByPrefix(InputPrefix)) {
        SetDataStatisticsField(result, TStringBuf(pair.first.begin() + 1 + InputPrefix.size(), pair.first.end()), pair.second.GetSum());
    }

    return result;
}

THashMap<int, TDataStatistics> GetOutputDataStatistics(const TStatistics& jobStatistics)
{
    THashMap<int, TDataStatistics> result;
    for (const auto& pair : jobStatistics.GetRangeByPrefix(OutputPrefix)) {
        TStringBuf currentPath(pair.first.begin() + OutputPrefix.size() + 1, pair.first.end());
        size_t slashPos = currentPath.find("/");
        if (slashPos == TStringBuf::npos) {
            // Looks like a malformed path in /data/output, let's skip it.
            continue;
        }
        int tableIndex = a2i(TString(currentPath.substr(0, slashPos)));
        SetDataStatisticsField(result[tableIndex], currentPath.substr(slashPos + 1), pair.second.GetSum());
    }

    return result;
}

THashMap<int, int> GetOutputPipeIdleTimes(const TStatistics& jobStatistics)
{
    THashMap<int, int> result;
    for (const auto& pair : jobStatistics.GetRangeByPrefix(OutputPipePrefix)) {
        const auto& path = pair.first;
        // Note that path should contain at least OutputPipePrefix + '/'.
        YCHECK(path.size() >= OutputPipePrefix.size() + 1);
        if (path.substr(path.size() - IdleTimeSuffix.size()) != IdleTimeSuffix) {
            continue;
        }
        int tableIndex = a2i(TString(path.begin() + OutputPipePrefix.size() + 1, path.end() - IdleTimeSuffix.size()));
        result[tableIndex] = pair.second.GetSum();
    }

    return result;
};

TDataStatistics GetTotalOutputDataStatistics(const TStatistics& jobStatistics)
{
    TDataStatistics result;
    for (const auto& pair : GetOutputDataStatistics(jobStatistics)) {
        result += pair.second;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

const TString ExecAgentTrafficStatisticsPrefix = "exec_agent";
const TString JobProxyTrafficStatisticsPrefix = "job_proxy";

void FillTrafficStatistics(
    const TString& namePrefix,
    NJobTrackerClient::TStatistics& statistics,
    const NChunkClient::TTrafficMeterPtr& trafficMeter)
{
    statistics.AddSample(
        Format("/%v/traffic/duration_ms", namePrefix),
        trafficMeter->GetDuration().MilliSeconds());

    // Empty data center names aren't allowed, so reducing a null data
    // center to an empty string is safe. And convenient :-)

    for (const auto& pair : trafficMeter->GetInboundByteCountBySource()) {
        auto dataCenter = pair.first ? pair.first : TString();
        auto byteCount = pair.second;
        statistics.AddSample(
            Format("/%v/traffic/inbound/from_%v", namePrefix, dataCenter),
            byteCount);
    }
    for (const auto& pair : trafficMeter->GetOutboundByteCountByDestination()) {
        auto dataCenter = pair.first ? pair.first : TString();
        auto byteCount = pair.second;
        statistics.AddSample(
            Format("/%v/traffic/outbound/to_%v", namePrefix, dataCenter),
            byteCount);
    }
    for (const auto& pair : trafficMeter->GetByteCountByDirection()) {
        auto srcDataCenter = pair.first.first ? pair.first.first : TString();
        auto dstDataCenter = pair.first.second ? pair.first.second : TString();
        auto byteCount = pair.second;
        statistics.AddSample(
            Format("/%v/traffic/%v_to_%v", namePrefix, srcDataCenter, dstDataCenter),
            byteCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

TStatisticsConsumer::TStatisticsConsumer(TSampleHandler sampleHandler)
    : TreeBuilder_(CreateBuilderFromFactory(GetEphemeralNodeFactory()))
    , SampleHandler_(sampleHandler)
{ }

void TStatisticsConsumer::OnMyListItem()
{
    TreeBuilder_->BeginTree();
    Forward(
        TreeBuilder_.get(),
        [this] {
            auto node = TreeBuilder_->EndTree();
            SampleHandler_.Run(node);
        },
        NYson::EYsonType::Node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
