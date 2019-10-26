#pragma once

#include "tablet_reader.h"

#include <yp/server/master/public.h>

#include <yt/client/api/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TWatchQueryEvent
{
    EEventType Type;
    TObjectId ObjectId;
    TTimestamp Timestamp;
    i64 RowIndex;
    i64 TabletIndex;
};

struct TWatchQueryOptions
{
    std::optional<TTimestamp> Timestamp;
    std::optional<TTimestamp> StartTimestamp;
    std::optional<TString> ContinuationToken;
    std::optional<i64> EventCountLimit;
    std::optional<TDuration> TimeLimit;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchQueryOptions& options,
    TStringBuf format);

struct TWatchQueryResult
{
    struct TEvent
    {
        TTimestamp Timestamp;
        EEventType EventType;
        TObjectId ObjectId;
    };

    TTimestamp Timestamp;
    std::vector<TWatchQueryEvent> Events;
    TString ContinuationToken;
};

////////////////////////////////////////////////////////////////////////////////

class TWatchLogReader
    : public TRefCounted
{
public:
    TWatchLogReader(
        TTimestamp timestamp,
        const TDBTable* table,
        const std::vector<NYT::NApi::TTabletInfo>& tabletInfos,
        ISession* const session);

    void ScheduleRead(
        TTimestamp startTimestamp);
    void ScheduleRead(
        const std::vector<i64>& tabletOffsets);

    TTimestamp GetTimestamp() const;
    std::vector<i64> GetTabletOffsets() const;

    std::vector<TWatchQueryEvent> Read(
        std::optional<i64> eventCountLimit);

private:
    const TTimestamp Timestamp_;
    ISession* const Session_;
    const i64 TabletCount_;
    std::vector<TTabletReaderPtr> TabletReaders_;
};

DEFINE_REFCOUNTED_TYPE(TWatchLogReader)

class TWatchQueryExecutor
    : public TRefCounted
{
public:
    TWatchQueryExecutor(
        NMaster::TBootstrap* bootstrap,
        ISession* session);

    TWatchQueryResult ExecuteWatchQuery(
        EObjectType type,
        const TWatchQueryOptions& options) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TWatchQueryExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
