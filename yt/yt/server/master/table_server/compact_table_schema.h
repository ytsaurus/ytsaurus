#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NTableServer {

///////////////////////////////////////////////////////////////////////////////

//! TCompactTableSchema is a light version of NTableClient::TTableSchema.
//! It stores minimum lightweighted and most-used fields of TTableSchema and compactified wire protobuf representation of schema itself.
//! This class is not formattable intentionally, since for formatting heavy TTableSchema should be parsed.
class TCompactTableSchema
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Empty, true);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Strict, false);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(UniqueKeys, false);

public:
    //! Constructs an empty non-strict schema.
    TCompactTableSchema();

    explicit TCompactTableSchema(const NTableClient::NProto::TTableSchemaExt& schema);
    explicit TCompactTableSchema(const NTableClient::TTableSchema& schema);
    explicit TCompactTableSchema(const NTableClient::TTableSchemaPtr& schema);

    bool operator==(const TCompactTableSchema& other) const;

    const std::string& AsWireProto() const;

    bool IsSorted() const;

    bool HasHunkColumns() const;
    bool HasNontrivialSchemaModification() const;

    int GetKeyColumnCount() const;
    const NTableClient::TKeyColumns& GetKeyColumns() const;
    const std::vector<NTableClient::ESortOrder>& GetSortOrders() const;

    i64 GetMemoryUsage() const;

    NTableClient::TComparator ToComparator(TCallback<NTableClient::TUUComparerSignature> cgComparator = {}) const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    friend void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchema& schema);

private:
    std::string TableSchema_;

    bool HasHunkColumns_ = false;
    bool HasNontrivialSchemaModification_ = false;
    NTableClient::TKeyColumns KeyColumns_;
    std::vector<NTableClient::ESortOrder> SortOrders_;

    void InitializePartial(
        bool empty,
        bool strict,
        bool uniqueKeys,
        NTableClient::ETableSchemaModification schemaModification);

    void InitializeFromProto(const NTableClient::NProto::TTableSchemaExt& protoSchema);

    void SerializeToProto(NTableClient::NProto::TTableSchemaExt* protoSchema) const;
};

DEFINE_REFCOUNTED_TYPE(TCompactTableSchema)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchema& schema);
void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

struct TCompactTableSchemaHash
{
    size_t operator()(const TCompactTableSchema& schema) const;
    size_t operator()(const TCompactTableSchemaPtr& schema) const;
};

struct TCompactTableSchemaEquals
{
    bool operator()(const TCompactTableSchema& lhs, const TCompactTableSchema& rhs) const;
    bool operator()(const TCompactTableSchemaPtr& lhs, const TCompactTableSchemaPtr& rhs) const;
    bool operator()(const TCompactTableSchemaPtr& lhs, const TCompactTableSchema& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NTableServer::TCompactTableSchema>
{
    size_t operator()(const NYT::NTableServer::TCompactTableSchema& tableSchema) const;
};

////////////////////////////////////////////////////////////////////////////////
