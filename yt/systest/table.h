#pragma once

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/systest/proto/table.pb.h>

namespace NYT::NTest {

struct TDataColumn {
    TString Name;
    NProto::EColumnType Type;
    std::optional<TString> StableName;
};

struct TTable {
    std::vector<TDataColumn> DataColumns;
    std::vector<TString> DeletedColumnNames;
    int SortColumns = 0;
};

////////////////////////////////////////////////////////////////////////////////

TString BuildAttributes(const TTable& table);

TString SchemaTypeName(NProto::EColumnType type);
NTableClient::ESimpleLogicalValueType GetType(NProto::EColumnType type);

void ToProto(NProto::TDataColumn* proto, const TDataColumn& column);
void FromProto(TDataColumn* column, const NProto::TDataColumn& proto);

void ToProto(NProto::TTable* proto, const TTable& table);
void FromProto(TTable* table, const NProto::TTable& proto);

void AlterTable(NApi::IClientPtr client, const TString& path, const TTable& table);
void FromTablePath(TTable* table, NApi::IClientPtr client, const TString& path);

}  // namespace NYT::NTest
