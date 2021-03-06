#pragma once

#include "public.h"

#include <yt/yt/client/table_client/schema.h>

#include <util/generic/hash_set.h>


namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TSharedTableSchemaRegistry
    : public TRefCounted
{
    friend class TSharedTableSchema;

public:
    TSharedTableSchemaPtr GetSchema(NTableClient::TTableSchema&& tableSchema);
    TSharedTableSchemaPtr GetSchema(const NTableClient::TTableSchema& tableSchema);
    size_t GetSize() const;
    void Clear();

    static const NTableClient::TTableSchema EmptyTableSchema;
    static const TFuture<NYson::TYsonString> EmptyYsonTableSchema;

private:
    void DropSchema(TSharedTableSchema* tableSchema);

private:
    struct TSharedTableSchemaHash
    {
        size_t operator() (const TSharedTableSchema* sharedTableSchema) const;
        size_t operator() (const NTableClient::TTableSchema& tableSchema) const;
    };

    struct TSharedTableSchemaEqual
    {
        bool operator() (const TSharedTableSchema* lhs, const TSharedTableSchema* rhs) const;
        bool operator() (const TSharedTableSchema* lhs, const NTableClient::TTableSchema& rhs) const;
    };

    using TProfilerSet = THashSet<TSharedTableSchema*, TSharedTableSchemaHash, TSharedTableSchemaEqual>;
    THashSet<TSharedTableSchema*, TSharedTableSchemaHash, TSharedTableSchemaEqual> Registry_;
};

DEFINE_REFCOUNTED_TYPE(TSharedTableSchemaRegistry);

////////////////////////////////////////////////////////////////////////////////

class TSharedTableSchema
    : public TRefCounted
{
public:
    TSharedTableSchema(NTableClient::TTableSchema tableSchema, const TSharedTableSchemaRegistryPtr& registry);
    ~TSharedTableSchema();

    const NTableClient::TTableSchema& GetTableSchema() const;
    const TFuture<NYson::TYsonString>& GetYsonTableSchema() const;
    size_t GetTableSchemaHash() const;

private:
    const NTableClient::TTableSchema TableSchema_;
    mutable TFuture<NYson::TYsonString> MemoizedYsonTableSchema_;
    const size_t TableSchemaHash_;
    const TSharedTableSchemaRegistryPtr Registry_;
};

DEFINE_REFCOUNTED_TYPE(TSharedTableSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
