#include "data_source.h"

#include <yt/ytlib/chunk_client/data_source.pb.h>

#include <yt/ytlib/table_client/schema_dictionary.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NYTree;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TDataSource::TDataSource(
    EDataSourceType type,
    const TNullable<TString>& path,
    const TNullable<TTableSchema>& schema,
    const TNullable<std::vector<TString>>& columns,
    TTimestamp timestamp)
    : Type_(type)
    , Path_(path)
    , Schema_(schema)
    , Columns_(columns)
    , Timestamp_(timestamp)
{ }

void ToProto(NProto::TDataSource* protoDataSource, const TDataSource& dataSource, TSchemaDictionary* dictionary)
{
    using NYT::ToProto;

    protoDataSource->set_type(static_cast<int>(dataSource.GetType()));

    if (dataSource.Schema()) {
        if (dictionary) {
            int id = dictionary->GetIdOrRegisterTable(*dataSource.Schema());
            protoDataSource->set_table_schema_id(id);
        } else {
            ToProto(protoDataSource->mutable_table_schema(), *dataSource.Schema());
        }
    }

    if (dataSource.Columns()) {
        protoDataSource->set_has_column_filter(true);
        ToProto(protoDataSource->mutable_columns(), *dataSource.Columns());
    }

    if (dataSource.GetPath()) {
        protoDataSource->set_path(*dataSource.GetPath());
    }

    if (dataSource.GetTimestamp()) {
        protoDataSource->set_timestamp(dataSource.GetTimestamp());
    }
}

void FromProto(TDataSource* dataSource, const NProto::TDataSource& protoDataSource, const TSchemaDictionary* dictionary)
{
    using NYT::FromProto;

    dataSource->SetType(EDataSourceType(protoDataSource.type()));

    if (dictionary) {
        if (protoDataSource.has_table_schema_id()) {
            int id = protoDataSource.table_schema_id();
            dataSource->Schema() = dictionary->GetTable(id);
        }

        YCHECK(!protoDataSource.has_table_schema());
    } else {
        if (protoDataSource.has_table_schema()) {
            dataSource->Schema() = FromProto<TTableSchema>(protoDataSource.table_schema());
        }

        YCHECK(!protoDataSource.has_table_schema_id());
    }

    if (protoDataSource.has_column_filter()) {
        dataSource->Columns() = FromProto<std::vector<TString>>(protoDataSource.columns());
    }

    if (protoDataSource.has_path()) {
        dataSource->SetPath(protoDataSource.path());
    }

    if (protoDataSource.has_timestamp()) {
        dataSource->SetTimestamp(protoDataSource.timestamp());
    }
}

TDataSource MakeVersionedDataSource(
    const TNullable<TString>& path,
    const NTableClient::TTableSchema& schema,
    const TNullable<std::vector<TString>>& columns,
    NTransactionClient::TTimestamp timestamp)
{
    return TDataSource(EDataSourceType::VersionedTable, path, schema, columns, timestamp);
}

TDataSource MakeUnversionedDataSource(
    const TNullable<TString>& path,
    const TNullable<NTableClient::TTableSchema>& schema,
    const TNullable<std::vector<TString>>& columns)
{
    return TDataSource(EDataSourceType::UnversionedTable, path, schema, columns, NullTimestamp);
}

TDataSource MakeFileDataSource(const TNullable<TString>& path)
{
    return TDataSource(EDataSourceType::File, path, Null, Null, NullTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDataSourceDirectoryExt* protoDataSourceDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory)
{
    using NYT::ToProto;
    TSchemaDictionary dictionary;
    for (const auto& dataSource : dataSourceDirectory->DataSources()) {
        auto* protoDataSource = protoDataSourceDirectory->add_data_sources();
        ToProto(protoDataSource, dataSource, &dictionary);
    }
    ToProto(protoDataSourceDirectory->mutable_schema_dictionary(), dictionary);
}

void FromProto(
    TDataSourceDirectoryPtr* dataSourceDirectory,
    const NProto::TDataSourceDirectoryExt& protoDataSourceDirectory)
{
    using NYT::FromProto;
    *dataSourceDirectory = New<TDataSourceDirectory>();
    auto& dataSources = (*dataSourceDirectory)->DataSources();
    TSchemaDictionary dictionary;
    FromProto(&dictionary, protoDataSourceDirectory.schema_dictionary());
    for (const auto& protoDataSource : protoDataSourceDirectory.data_sources()) {
        TDataSource dataSource;
        FromProto(&dataSource, protoDataSource, & dictionary);
        dataSources.emplace_back(std::move(dataSource));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

