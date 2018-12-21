#include "data_source.h"

#include <yt/ytlib/chunk_client/data_source.pb.h>

#include <yt/ytlib/table_client/schema_dictionary.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

using namespace NYTree;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TDataSource::TDataSource(
    EDataSourceType type,
    const std::optional<TString>& path,
    const std::optional<TTableSchema>& schema,
    const std::optional<std::vector<TString>>& columns,
    TTimestamp timestamp,
    const TColumnRenameDescriptors& columnRenameDescriptors)
    : Type_(type)
    , Path_(path)
    , Schema_(schema)
    , Columns_(columns)
    , Timestamp_(timestamp)
    , ColumnRenameDescriptors_(columnRenameDescriptors)
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

    protoDataSource->set_foreign(dataSource.GetForeign());
    ToProto(protoDataSource->mutable_column_rename_descriptors(), dataSource.ColumnRenameDescriptors());
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

    if (protoDataSource.has_foreign()) {
        dataSource->SetForeign(protoDataSource.foreign());
    }

    dataSource->ColumnRenameDescriptors() = FromProto<TColumnRenameDescriptors>(protoDataSource.column_rename_descriptors());
}

TDataSource MakeVersionedDataSource(
    const std::optional<TString>& path,
    const NTableClient::TTableSchema& schema,
    const std::optional<std::vector<TString>>& columns,
    NTransactionClient::TTimestamp timestamp,
    const TColumnRenameDescriptors& columnRenameDescriptors)
{
    return TDataSource(EDataSourceType::VersionedTable, path, schema, columns, timestamp, columnRenameDescriptors);
}

TDataSource MakeUnversionedDataSource(
    const std::optional<TString>& path,
    const std::optional<NTableClient::TTableSchema>& schema,
    const std::optional<std::vector<TString>>& columns,
    const TColumnRenameDescriptors& columnRenameDescriptors)
{
    return TDataSource(EDataSourceType::UnversionedTable, path, schema, columns, NullTimestamp, columnRenameDescriptors);
}

TDataSource MakeFileDataSource(const std::optional<TString>& path)
{
    return TDataSource(EDataSourceType::File, path, std::nullopt, std::nullopt, NullTimestamp, {});
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

} // namespace NYT::NChunkClient

