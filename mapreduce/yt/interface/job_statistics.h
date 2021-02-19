#pragma once

#include "fwd.h"

#include <library/cpp/yson/node/node.h>

#include <util/system/defaults.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////

class TNode;

template <typename T>
class TJobStatisticsEntry;

////////////////////////////////////////////////////////////////////

//
// Function converts i64 representation of statistics to other type.
// Library defines this template for types TDuration and i64.
// Users may define it for their types.
//
// Check TJobStatistics::GetStatisticsAs method.
template <typename T>
T ConvertJobStatisticsEntry(i64 value);

////////////////////////////////////////////////////////////////////

class TJobStatistics
{
public:
    //
    // Construct empty statistics.
    TJobStatistics();

    //
    // Construct statistcs from statistics node.
    TJobStatistics(const NYT::TNode& statistics);

    TJobStatistics(const TJobStatistics& jobStatistics);
    TJobStatistics(TJobStatistics&& jobStatistics);

    TJobStatistics& operator=(const TJobStatistics& jobStatistics);
    TJobStatistics& operator=(TJobStatistics&& jobStatistics);

    ~TJobStatistics();

    //
    // Filter statistics by task name.
    // By default filter includes all tasks.
    // Specify empty `filter' to include all tasks.
    TJobStatistics TaskName(TVector<TTaskName> taskNames) const;

    //
    // Filter statistics by job state.
    // By default filter includes only (successfuly) completed jobs.
    // Specify empty `filter' to include all job states.
    TJobStatistics JobState(TVector<EJobState> filter) const;

    //
    // DEPRECATED, USE TaskName INSTEAD!
    // SEE https://yt.yandex-team.ru/docs/description/mr/jobs#obshaya-shema
    //
    // Filter statistics by job type.
    // By default filter includes all job types.
    // Specify empty `filter' to include all job types.
    TJobStatistics JobType(TVector<EJobType> filter) const;

    //
    // Check that given statistics exist.
    //
    // Slash separated statistics name should be used e.g. "time/total" (like it appears in web interface).
    bool HasStatistics(TStringBuf name) const;

    //
    // Get statistics by name.
    //
    // Slash separated statistics name should be used e.g. "time/total" (like it appears in web interface).
    //
    // If statistics is missing an exception is thrown. If because of filters
    // no fields remain the returned value is empty (all fields are Nothing).
    //
    // In order to use GetStatisticsAs method, ConvertJobStatisticsEntry function must be defined.
    // (Library defines it for i64 and TDuration, user may define it for other types).
    //
    // NOTE: We don't use TMaybe<TJobStatisticsEntry> here instead TJobStatisticsEntry return TMaybe<i64>,
    // so user easier use GetOrElse:
    //     jobStatistics.GetStatistics("some/statistics/name").Max().GetOrElse(0);
    TJobStatisticsEntry<i64> GetStatistics(TStringBuf name) const;

    template <typename T>
    TJobStatisticsEntry<T> GetStatisticsAs(TStringBuf name) const;

    //
    // Get (slash separated) names of statistics.
    TVector<TString> GetStatisticsNames() const;

    //
    // Check if given custom statistics exists.
    bool HasCustomStatistics(TStringBuf name) const;

    //
    // Get custom statistics (those the user can write in operation with WriteCustomStatistics).
    TJobStatisticsEntry<i64> GetCustomStatistics(TStringBuf name) const;

    template <typename T>
    TJobStatisticsEntry<T> GetCustomStatisticsAs(TStringBuf name) const;

    //
    // Get names of all custom statistics.
    TVector<TString> GetCustomStatisticsNames() const;

private:
    class TData;
    struct TFilter;

    struct TDataEntry {
        i64 Max;
        i64 Min;
        i64 Sum;
        i64 Count;
    };

    static const TString CustomStatisticsNamePrefix_;

private:
    TJobStatistics(::TIntrusivePtr<TData> data, ::TIntrusivePtr<TFilter> filter);

    TMaybe<TDataEntry> GetStatisticsImpl(TStringBuf name) const;

private:
    ::TIntrusivePtr<TData> Data_;
    ::TIntrusivePtr<TFilter> Filter_;

private:
    template<typename T>
    friend class TJobStatisticsEntry;
};

////////////////////////////////////////////////////////////////////

template <typename T>
class TJobStatisticsEntry
{
public:
    TJobStatisticsEntry(TMaybe<TJobStatistics::TDataEntry> data)
        : Data_(std::move(data))
    { }

    TMaybe<T> Sum() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Sum);
        }
        return Nothing();
    }

    //
    // NOTE: Only jobs that emitted statistics are taken into account.
    TMaybe<T> Avg() const
    {
        if (Data_ && Data_->Count) {
            return ConvertJobStatisticsEntry<T>(Data_->Sum / Data_->Count);
        }
        return Nothing();
    }

    TMaybe<T> Count() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Count);
        }
        return Nothing();
    }

    TMaybe<T> Max() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Max);
        }
        return Nothing();
    }

    TMaybe<T> Min() const
    {
        if (Data_) {
            return ConvertJobStatisticsEntry<T>(Data_->Min);
        }
        return Nothing();
    }

private:
    TMaybe<TJobStatistics::TDataEntry> Data_;

private:
    friend class TJobStatistics;
};

////////////////////////////////////////////////////////////////////

template <typename T>
TJobStatisticsEntry<T> TJobStatistics::GetStatisticsAs(TStringBuf name) const
{
    return TJobStatisticsEntry<T>(GetStatisticsImpl(name));
}

template <typename T>
TJobStatisticsEntry<T> TJobStatistics::GetCustomStatisticsAs(TStringBuf name) const
{
    return TJobStatisticsEntry<T>(GetStatisticsImpl(CustomStatisticsNamePrefix_ + name));
}

////////////////////////////////////////////////////////////////////

//
// Write custom statistics (see https://yt.yandex-team.ru/docs/description/mr/jobs#user_stats)
// by given slash-separated path (its length must not exceed 512 bytes).
// The function must be called in job.
// Total number of statistics (with different paths) must not exceed 128.
void WriteCustomStatistics(TStringBuf path, i64 value);

//
// Write several custom statistics at once.
// The argument must be a map with leaves of type i64.
// Equivalent to calling WriteCustomStatistics(path, value) for every path in the given map.
void WriteCustomStatistics(const TNode& statistics);

////////////////////////////////////////////////////////////////////

} // namespace NYT
