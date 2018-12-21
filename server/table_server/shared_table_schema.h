#pragma once

#include "public.h"

#include <yt/client/table_client/schema.h>

#include <util/generic/hash_set.h>


namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TSharedTableSchemaRegistry
    : public TIntrinsicRefCounted
{
    friend class TSharedTableSchema;

public:
    TSharedTableSchemaPtr GetSchema(NTableClient::TTableSchema&& tableSchema);
    size_t GetSize() const;
    void Clear();

    static const NTableClient::TTableSchema EmptyTableSchema;

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

    using TRegistrySet = THashSet<TSharedTableSchema*, TSharedTableSchemaHash, TSharedTableSchemaEqual>;
    THashSet<TSharedTableSchema*, TSharedTableSchemaHash, TSharedTableSchemaEqual> Registry_;
};

DEFINE_REFCOUNTED_TYPE(TSharedTableSchemaRegistry);

////////////////////////////////////////////////////////////////////////////////

class TSharedTableSchema
    : public TIntrinsicRefCounted
{
public:
    TSharedTableSchema(NTableClient::TTableSchema tableSchema, const TSharedTableSchemaRegistryPtr& registry);
    ~TSharedTableSchema();

    const NTableClient::TTableSchema& GetTableSchema() const;
    size_t GetTableSchemaHash() const;

public:
    const NTableClient::TTableSchema TableSchema_;
    const size_t TableSchemaHash_;
    const TSharedTableSchemaRegistryPtr Registry_;
};

DEFINE_REFCOUNTED_TYPE(TSharedTableSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
