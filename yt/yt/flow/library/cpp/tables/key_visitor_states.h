#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

struct IKeyVisitorStates
    : public TRefCounted
{
    struct TTableKey
    {
        TComputationId ComputationId;
        TStreamId StreamId;
        TKey Key;
        //! True for the inclusive lower-bound row of an interval, false for
        //! the exclusive upper-bound row. Stored ascending so that scanning
        //! `[(A, true) ... (B, false)]` yields the interval `[A, B)`.
        bool IsLower{};

        bool operator==(const TTableKey& other) const = default;
    };

    //! Both bounds are inclusive: scanning `[(A, IsLower=true) ... (B, IsLower=false)]`
    //! returns the rows that bracket the half-open partition range `[A; B)`.
    struct TTableKeyFilter
    {
        std::optional<TComputationId> ComputationId;
        std::optional<TStreamId> StreamId;
        std::optional<TKey> LowerKey;
        std::optional<TKey> UpperKey;
    };

    using TValue = NYson::TYsonString;

    struct TReadResult
    {
        std::vector<std::pair<TTableKey, TValue>> Rows;
        std::optional<TTableKey> ContinuationOffsetExclusive;
    };

    virtual void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) = 0;

    //! Writes a batch of mutations.
    //! `nullopt` value means "delete row at this key".
    virtual void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<std::pair<TTableKey, std::optional<TValue>>>& mutations) = 0;

    virtual TFuture<TReadResult> Read(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) = 0;

    virtual TFuture<std::vector<std::pair<TTableKey, TValue>>> ReadAll(TTableKeyFilter filter) = 0;
};

DEFINE_REFCOUNTED_TYPE(IKeyVisitorStates);

////////////////////////////////////////////////////////////////////////////////

class TKeyVisitorStates
    : public IKeyVisitorStates
{
public:
    TKeyVisitorStates(TContextPtr context, TDynamicTableRequestSpecPtr dynamicSpec);

    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<std::pair<TTableKey, std::optional<TValue>>>& mutations) override;

    TFuture<TReadResult> Read(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override;

    TFuture<std::vector<std::pair<TTableKey, TValue>>> ReadAll(TTableKeyFilter filter) override;

private:
    const TContextPtr Context_;
    TDynamicTableRequestSpecPtr DynamicSpec_;
    const NYPath::TYPath TablePath_;
    const NLogging::TLogger Logger;

    NProfiling::TCounter SelectRows_;
    NProfiling::TCounter SelectBytes_;
    NProfiling::TCounter WriteRows_;
    NProfiling::TCounter WriteBytes_;
    NProfiling::TCounter UpdateRows_;
    NProfiling::TCounter EraseRows_;
    NProfiling::TEventTimer SelectTime_;
};

DEFINE_REFCOUNTED_TYPE(TKeyVisitorStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

template <>
struct THash<NYT::NFlow::NTables::IKeyVisitorStates::TTableKey>
{
    size_t operator()(const NYT::NFlow::NTables::IKeyVisitorStates::TTableKey& tableKey) const;
};
