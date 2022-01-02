#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/client/api/client.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! A simple typed interface for accessing given state table. All methods are thread-safe.
template <class TRow, class TId>
class TTableBase
    : public TRefCounted
{
public:
    TTableBase(NYPath::TYPath path, NApi::IClientPtr client);

    TFuture<std::vector<TRow>> Select(TStringBuf columns = "*", TStringBuf where = "1 = 1") const;

private:
    NYPath::TYPath Path_;
    NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

struct TQueueId
{
    TString Cluster;
    NYPath::TYPath Path;

    bool operator ==(const TQueueId& other) const;
};

TString ToString(const TQueueId& queueId);

void Serialize(const TQueueId& queueId, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

// Keep in-sync with #TQueueTableDescriptor, #TQueueTableRow::ParseRowRange and #Serialize.
struct TQueueTableRow
{
    TQueueId QueueId;
    // Even though some fields are nullable by their nature (e.g. revision),
    // outer-level nullopt is interpreted as Null, i.e. missing value.
    std::optional<NHydra::TRevision> Revision;
    std::optional<NObjectClient::EObjectType> ObjectType;
    std::optional<bool> Dynamic;
    std::optional<bool> Sorted;

    static std::vector<TQueueTableRow> ParseRowRange(
        TRange<NTableClient::TUnversionedRow> rows,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TTableSchema& schema);
};

void Serialize(const TQueueTableRow& row, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TQueueTable
    : public TTableBase<TQueueTableRow, TQueueId>
{
public:
    TQueueTable(NYPath::TYPath root, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TQueueTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

template <>
struct THash<NYT::NQueueAgent::TQueueId>
{
    size_t operator()(const NYT::NQueueAgent::TQueueId& queueId) const;
};
