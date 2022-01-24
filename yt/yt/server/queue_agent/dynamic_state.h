#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/client/api/client.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct TCrossClusterReference
{
    TString Cluster;
    NYPath::TYPath Path;

    bool operator ==(const TCrossClusterReference& other) const;

    static TCrossClusterReference FromString(const TString& path);
};

TString ToString(const TCrossClusterReference& queueRef);

void Serialize(const TCrossClusterReference& queueRef, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! A simple typed interface for accessing given state table. All methods are thread-safe.
template <class TRow>
class TTableBase
    : public TRefCounted
{
public:
    TTableBase(NYPath::TYPath path, NApi::IClientPtr client);

    TFuture<std::vector<TRow>> Select(TStringBuf columns = "*", TStringBuf where = "1 = 1") const;

    TFuture<NApi::TTransactionCommitResult> Insert(std::vector<TRow> rows) const;

private:
    NYPath::TYPath Path_;
    NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

// Keep fields in-sync with the implementations of all related methods in the corresponding cpp file.
struct TQueueTableRow
{
    TCrossClusterReference Queue;
    std::optional<TRowRevision> RowRevision;
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

    static void InsertRowRange(
        const NYPath::TYPath& path,
        TRange<TQueueTableRow> rows,
        const NApi::ITransactionPtr& transaction);

    static std::vector<TString> GetCypressAttributeNames();

    TQueueTableRow() = default;
    TQueueTableRow(
        TCrossClusterReference queue,
        std::optional<TRowRevision> rowRevision,
        const NYTree::IAttributeDictionaryPtr& attributeDictionary);
};

void Serialize(const TQueueTableRow& row, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TQueueTable
    : public TTableBase<TQueueTableRow>
{
public:
    TQueueTable(NYPath::TYPath root, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TQueueTable)

////////////////////////////////////////////////////////////////////////////////

// Keep fields in-sync with the implementations of all related methods in the corresponding cpp file.
struct TConsumerTableRow
{
    TCrossClusterReference Consumer;
    std::optional<TRowRevision> RowRevision;
    // Even though some fields are nullable by their nature (e.g. revision),
    // outer-level nullopt is interpreted as Null, i.e. missing value.
    std::optional<TCrossClusterReference> Target;
    std::optional<NHydra::TRevision> Revision;
    std::optional<NObjectClient::EObjectType> ObjectType;
    std::optional<bool> TreatAsConsumer;

    static std::vector<TConsumerTableRow> ParseRowRange(
        TRange<NTableClient::TUnversionedRow> rows,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TTableSchema& schema);

    static void InsertRowRange(
        const NYPath::TYPath& path,
        TRange<TConsumerTableRow> rows,
        const NApi::ITransactionPtr& transaction);

    static std::vector<TString> GetCypressAttributeNames();

    TConsumerTableRow() = default;
    TConsumerTableRow(
        TCrossClusterReference consumer,
        std::optional<TRowRevision> rowRevision,
        const NYTree::IAttributeDictionaryPtr& attributeDictionary);
};

void Serialize(const TConsumerTableRow& row, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TConsumerTable
    : public TTableBase<TConsumerTableRow>
{
public:
    TConsumerTable(NYPath::TYPath root, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TConsumerTable)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicState
    : public TRefCounted
{
    TQueueTablePtr Queues;
    TConsumerTablePtr Consumers;

    TDynamicState(NYPath::TYPath root, NApi::IClientPtr client);
};

DEFINE_REFCOUNTED_TYPE(TDynamicState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

template <>
struct THash<NYT::NQueueAgent::TCrossClusterReference>
{
    size_t operator()(const NYT::NQueueAgent::TCrossClusterReference& queueRef) const;
};
