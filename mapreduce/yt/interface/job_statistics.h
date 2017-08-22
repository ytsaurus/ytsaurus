#pragma once

#include "fwd.h"

#include <util/system/defaults.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////

class TNode;

template <typename T>
class TJobStatisticsEntry;

////////////////////////////////////////////////////////////////////

enum EFinishedJobState : int
{
    FJS_COMPLETED /* "completed" */,
    FJS_ABORTED   /* "aborted" */,
    FJS_FAILED    /* "failed" */,
    FJS_LOST      /* "lost" */,
};

enum EJobType : int
{
    JT_MAP               /* "map" */,
    JT_PARTITION_MAP     /* "partition_map" */,
    JT_SORTED_MERGE      /* "sorted_merge" */,
    JT_ORDERED_MERGE     /* "ordered_merge" */,
    JT_UNORDERED_MERGE   /* "unordered_merge" */,
    JT_PARTITION         /* "partition" */,
    JT_SIMPLE_SORT       /* "simple_sort" */,
    JT_FINAL_SORT        /* "final_sort" */,
    JT_SORTED_REDUCE     /* "sorted_reduce" */,
    JT_PARTITION_REDUCE  /* "partition_reduce" */,
    JT_REDUCE_COMBINER   /* "reduce_combiner" */,
    JT_REMOTE_COPY       /* "remote_copy" */,
    JT_INTERMEDIATE_SORT /* "intermediate_sort" */,
    JT_ORDERED_MAP       /* "ordered_map" */,
    JT_JOIN_REDUCE       /* "join_reduce" */,
};

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
    // Construct empty statistics.
    TJobStatistics();

    // Construct statistcs from statistics node.
    // Such statistics node can be read from path:
    //   //sys/operations/<operation-id>/@progress/job_statistics
    TJobStatistics(const NYT::TNode& statistics);

    TJobStatistics(const TJobStatistics& jobStatistics);
    TJobStatistics(TJobStatistics&& jobStatistics);

    TJobStatistics& operator=(const TJobStatistics& jobStatistics);
    TJobStatistics& operator=(TJobStatistics&& jobStatistics);

    ~TJobStatistics();

    // Filter statistics by job type.
    // By default filter includes all job types.
    // Specify empty `filter' to include all job types.
    TJobStatistics JobType(yvector<EJobType> filter) const;

    // Filter statistics by job state.
    // By default filter includes only (successfuly) completed jobs.
    // Specify empty `filter' to include all job states.
    TJobStatistics JobStatus(yvector<EFinishedJobState> filter) const;

    // Get statistics by name.
    // If statistics is missing returned value is empty (all fields are Nothing).
    //
    // In order to use GetStatisticsAs method, ConvertJobStatisticsEntry function must be defined.
    // (Library defines it for i64 and TDuration, user may define it for other types).
    TJobStatisticsEntry<i64> GetStatistics(TStringBuf name) const;

    template <typename T>
    TJobStatisticsEntry<T> GetStatisticsAs(TStringBuf name) const;

private:
    class TData;
    struct TFilter;

    struct TDataEntry {
        i64 Max;
        i64 Min;
        i64 Sum;
        i64 Count;
    };

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

    // NOTE: Only jobs that emited statistics are taken into account.
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

////////////////////////////////////////////////////////////////////

} // namespace NYT
