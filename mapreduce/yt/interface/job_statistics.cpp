#include "job_statistics.h"

#include "operation.h"

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/serialize.h>

#include <library/yson/writer.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/subst.h>
#include <util/system/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////

template <>
i64 ConvertJobStatisticsEntry(i64 value)
{
    return value;
}

template <>
TDuration ConvertJobStatisticsEntry(i64 value)
{
    return TDuration::MilliSeconds(value);
}

////////////////////////////////////////////////////////////////////

class TJobStatistics::TData
    : public TThrRefBase
{
public:
    using TType2Data = THashMap<EJobType, TJobStatistics::TDataEntry>;
    using TState2Type2Data = THashMap<EJobState, TType2Data>;
    using TName2State2Type2Data = THashMap<TString, TState2Type2Data>;

public:
    TName2State2Type2Data Name2State2Type2Data;

public:
    TData() = default;

    TData(const TNode& statisticsNode)
    {
        ParseNode(statisticsNode, TString(), &Name2State2Type2Data);
    }

    static void Aggregate(TJobStatistics::TDataEntry* result, const TJobStatistics::TDataEntry& other)
    {
        result->Max = Max(result->Max, other.Max);
        result->Min = Min(result->Min, other.Min);
        result->Sum += other.Sum;
        result->Count += other.Count;
    }

    static void ParseNode(const TNode& node, TState2Type2Data* output)
    {
        auto getInt = [] (const TNode& theNode, TStringBuf key) {
            const auto& nodeAsMap = theNode.AsMap();
            auto it = nodeAsMap.find(key);
            if (it == nodeAsMap.end()) {
                ythrow yexception() << "Key '" << key << "' is not found";
            }
            const auto& valueNode = it->second;
            if (!valueNode.IsInt64()) {
                ythrow yexception() << "Key '" << key << "' is not of int64 type";
            }
            return valueNode.AsInt64();
        };

        for (const auto& item1 : node.AsMap()) {
            const auto& stateStr = item1.first;
            EJobState state;
            if (!TryFromString(stateStr, state)) {
                continue;
            }
            const auto& type2DataNode = item1.second;
            for (const auto& item2 : type2DataNode.AsMap()) {
                const auto& typeStr = item2.first;
                EJobType type;
                if (!TryFromString(typeStr, type)) {
                    continue;
                }
                const auto& dataNode = item2.second;

                auto& data = (*output)[state][type];

                data.Max = getInt(dataNode, "max");
                data.Min = getInt(dataNode, "min");
                data.Sum = getInt(dataNode, "sum");
                data.Count = getInt(dataNode, "count");
            }
        }
    }

    static void ParseNode(const TNode& node, const TString& curPath, TName2State2Type2Data* output)
    {
        Y_VERIFY(node.IsMap());

        for (const auto& child : node.AsMap()) {
            const auto& key = child.first;
            const auto& value = child.second;
            if (key == AsStringBuf("$")) {
                ParseNode(value, &(*output)[curPath]);
            } else {
                TString childPath = curPath;
                if (!childPath.empty()) {
                    childPath.push_back('/');
                }
                if (key.find_first_of('/') != key.npos) {
                    TString keyCopy(key);
                    SubstGlobal(keyCopy, "/", "\\/");
                    childPath += keyCopy;
                } else {
                    childPath += key;
                }
                ParseNode(value, childPath, output);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////

struct TJobStatistics::TFilter
    : public TThrRefBase
{
    TVector<EJobType> JobTypeFilter;
    TVector<EJobState> JobStateFilter = {EJobState::Completed};
};

////////////////////////////////////////////////////////////////////

const TString TJobStatistics::CustomStatisticsNamePrefix_ = "custom/";

TJobStatistics::TJobStatistics()
    : Data_(::MakeIntrusive<TData>())
    , Filter_(::MakeIntrusive<TFilter>())
{ }


TJobStatistics::TJobStatistics(const NYT::TNode& statisticsNode)
    : Data_(::MakeIntrusive<TData>(statisticsNode))
    , Filter_(::MakeIntrusive<TFilter>())
{ }

TJobStatistics::TJobStatistics(::TIntrusivePtr<TData> data, ::TIntrusivePtr<TFilter> filter)
    : Data_(data)
    , Filter_(filter)
{ }

TJobStatistics::TJobStatistics(const TJobStatistics& jobStatistics) = default;
TJobStatistics::TJobStatistics(TJobStatistics&&) = default;

TJobStatistics& TJobStatistics::operator=(const TJobStatistics& jobStatistics) = default;
TJobStatistics& TJobStatistics::operator=(TJobStatistics&& jobStatistics) = default;

TJobStatistics::~TJobStatistics() = default;

TJobStatistics TJobStatistics::JobType(TVector<EJobType> filter) const
{
    auto newFilter = ::MakeIntrusive<TFilter>();
    newFilter->JobTypeFilter = std::move(filter);
    newFilter->JobStateFilter = Filter_->JobStateFilter;

    return TJobStatistics(Data_, std::move(newFilter));
}

TJobStatistics TJobStatistics::JobState(TVector<EJobState> filter) const
{
    auto newFilter = ::MakeIntrusive<TFilter>();
    newFilter->JobTypeFilter = Filter_->JobTypeFilter;
    newFilter->JobStateFilter = std::move(filter);

    return TJobStatistics(Data_, std::move(newFilter));
}

bool TJobStatistics::HasStatistics(TStringBuf name) const
{
    return Data_->Name2State2Type2Data.contains(name);
}

TJobStatisticsEntry<i64> TJobStatistics::GetStatistics(TStringBuf name) const
{
    return GetStatisticsAs<i64>(name);
}

TVector<TString> TJobStatistics::GetStatisticsNames() const
{
    TVector<TString> result;
    result.reserve(Data_->Name2State2Type2Data.size());
    for (const auto& entry : Data_->Name2State2Type2Data) {
        result.push_back(entry.first);
    }
    return result;
}

bool TJobStatistics::HasCustomStatistics(TStringBuf name) const
{
    return HasStatistics(CustomStatisticsNamePrefix_ + name);
}

TJobStatisticsEntry<i64> TJobStatistics::GetCustomStatistics(TStringBuf name) const
{
    return GetCustomStatisticsAs<i64>(name);
}

TVector<TString> TJobStatistics::GetCustomStatisticsNames() const
{
    TVector<TString> result;
    for (const auto& entry : Data_->Name2State2Type2Data) {
        if (entry.first.StartsWith(CustomStatisticsNamePrefix_)) {
            result.push_back(entry.first.substr(CustomStatisticsNamePrefix_.size()));
        }
    }
    return result;
}

TMaybe<TJobStatistics::TDataEntry> TJobStatistics::GetStatisticsImpl(TStringBuf name) const
{
    auto name2State2Type2DataIt = Data_->Name2State2Type2Data.find(name);
    Y_ENSURE(name2State2Type2DataIt != Data_->Name2State2Type2Data.end(), "Statistics '" << name << "' are missing");
    const auto& state2Type2Data = name2State2Type2DataIt->second;

    TMaybe<TDataEntry> result;
    auto aggregate = [&] (const TDataEntry& data) {
        if (result) {
            TData::Aggregate(&result.GetRef(), data);
        } else {
            result = data;
        }
    };

    auto aggregateType2Data = [&] (const TData::TType2Data& type2Data) {
        if (Filter_->JobTypeFilter.empty()) {
            for (const auto& item : type2Data) {
                const auto& data = item.second;
                aggregate(data);
            }
        } else {
            for (const auto& type : Filter_->JobTypeFilter) {
                auto it = type2Data.find(type);
                if (it == type2Data.end()) {
                    continue;
                }
                const auto& data = it->second;
                aggregate(data);
            }
        }
    };

    if (Filter_->JobStateFilter.empty()) {
        for (const auto& item: state2Type2Data) {
            const auto& type2Data = item.second;
            aggregateType2Data(type2Data);
        }
    } else {
        for (auto state : Filter_->JobStateFilter) {
            auto it = state2Type2Data.find(state);
            if (it == state2Type2Data.end()) {
                continue;
            }
            const auto& type2Data = it->second;
            aggregateType2Data(type2Data);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////

namespace {

constexpr int USER_STATISTICS_FILE_DESCRIPTOR = 5;
constexpr char PATH_DELIMITER = '/';
constexpr char ESCAPE = '\\';

IOutputStream* GetStatisticsStream()
{
    static TFile file = Duplicate(USER_STATISTICS_FILE_DESCRIPTOR);
    static TFileOutput stream(file);
    return &stream;
}

template <typename T>
void WriteCustomStatisticsAny(TStringBuf path, const T& value)
{
    TYsonWriter writer(GetStatisticsStream(), YF_BINARY, YT_LIST_FRAGMENT);
    int depth = 0;
    size_t begin = 0;
    size_t end = 0;
    TVector<TString> items;
    while (end <= path.size()) {
        if (end + 1 < path.size() && path[end] == ESCAPE && path[end + 1] == PATH_DELIMITER) {
            end += 2;
            continue;
        }
        if (end == path.size() || path[end] == PATH_DELIMITER) {
            writer.OnBeginMap();
            items.emplace_back(path.data() + begin, end - begin);
            SubstGlobal(items.back(), "\\/", "/");
            writer.OnKeyedItem(TStringBuf(items.back()));
            ++depth;
            begin = end + 1;
        }
        ++end;
    }
    Serialize(value, &writer);
    while (depth > 0) {
        writer.OnEndMap();
        --depth;
    }
}

}

////////////////////////////////////////////////////////////////////

void WriteCustomStatistics(const TNode& statistics)
{
    TYsonWriter writer(GetStatisticsStream(), YF_BINARY, YT_LIST_FRAGMENT);
    Serialize(statistics, &writer);
}

void WriteCustomStatistics(TStringBuf path, i64 value)
{
    WriteCustomStatisticsAny(path, value);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT
