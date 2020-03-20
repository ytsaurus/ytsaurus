#include "data_source.h"

#include <yt/ytlib/chunk_client/proto/data_source.pb.h>

#include <yt/ytlib/table_client/schema_dictionary.h>
#include <yt/ytlib/table_client/column_filter_dictionary.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

using namespace NYTree;
using namespace NYPath;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TDataSource::TDataSource(
    EDataSourceType type,
    const std::optional<TYPath>& path,
    const std::optional<TTableSchema>& schema,
    const std::optional<std::vector<TString>>& columns,
    const std::vector<TString>& omittedInaccessibleColumns,
    TTimestamp timestamp,
    TTimestamp retentionTimestamp,
    const TColumnRenameDescriptors& columnRenameDescriptors)
    : Type_(type)
    , Path_(path)
    , Schema_(schema)
    , Columns_(columns)
    , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
    , Timestamp_(timestamp)
    , RetentionTimestamp_(retentionTimestamp)
    , ColumnRenameDescriptors_(columnRenameDescriptors)
{ }

void ToProto(NProto::TDataSource* protoDataSource, const TDataSource& dataSource, TSchemaDictionary* schemaDictionary, TColumnFilterDictionary* columnFilterDictionary)
{
    using NYT::ToProto;

    protoDataSource->set_type(static_cast<int>(dataSource.GetType()));

    if (dataSource.Schema()) {
        if (schemaDictionary) {
            int id = schemaDictionary->GetIdOrRegisterTable(*dataSource.Schema());
            protoDataSource->set_table_schema_id(id);
        } else {
            ToProto(protoDataSource->mutable_table_schema(), *dataSource.Schema());
        }
    }

    if (dataSource.Columns()) {
        if (columnFilterDictionary) {
            int id = columnFilterDictionary->GetIdOrRegisterAdmittedColumns(*dataSource.Columns());
            protoDataSource->set_column_filter_id(id);
        } else {
            ToProto(protoDataSource->mutable_column_filter()->mutable_admitted_names(), *dataSource.Columns());
        }

        // COMPAT(evgenstf): YT-11994: remove this old format support.
        protoDataSource->set_has_legacy_column_filter(true);
        ToProto(protoDataSource->mutable_legacy_admitted_columns(), *dataSource.Columns());
    }

    ToProto(protoDataSource->mutable_omitted_inaccessible_columns(), dataSource.OmittedInaccessibleColumns());

    if (dataSource.GetPath()) {
        protoDataSource->set_path(*dataSource.GetPath());
    }

    if (dataSource.GetTimestamp() != NullTimestamp) {
        protoDataSource->set_timestamp(dataSource.GetTimestamp());
    }

    if (dataSource.GetRetentionTimestamp() != NullTimestamp) {
        protoDataSource->set_retention_timestamp(dataSource.GetRetentionTimestamp());
    }

    protoDataSource->set_foreign(dataSource.GetForeign());

    ToProto(protoDataSource->mutable_column_rename_descriptors(), dataSource.ColumnRenameDescriptors());
}

void FromProto(TDataSource* dataSource, const NProto::TDataSource& protoDataSource, const TSchemaDictionary* schemaDictionary, const TColumnFilterDictionary* columnFilterDictionary)
{
    using NYT::FromProto;

    dataSource->SetType(EDataSourceType(protoDataSource.type()));

    if (schemaDictionary) {
        if (protoDataSource.has_table_schema_id()) {
            int id = protoDataSource.table_schema_id();
            dataSource->Schema() = schemaDictionary->GetTable(id);
        }

        YT_VERIFY(!protoDataSource.has_table_schema());
    } else {
        if (protoDataSource.has_table_schema()) {
            dataSource->Schema() = FromProto<TTableSchema>(protoDataSource.table_schema());
        }

        YT_VERIFY(!protoDataSource.has_table_schema_id());
    }

    if (protoDataSource.has_column_filter()) {
        YT_VERIFY(!protoDataSource.has_column_filter_id());
        dataSource->Columns() = FromProto<std::vector<TString>>(protoDataSource.column_filter().admitted_names());
    } else if (protoDataSource.has_column_filter_id()) {
        YT_VERIFY(columnFilterDictionary);
        int id = protoDataSource.column_filter_id();
        dataSource->Columns() = columnFilterDictionary->GetAdmittedColumns(id);
    }
    // COMPAT(evgenstf): YT-11994: remove this old format support.
    else if (
        protoDataSource.has_has_legacy_column_filter() &&
        protoDataSource.has_legacy_column_filter())
    {
        dataSource->Columns() = FromProto<std::vector<TString>>(protoDataSource.legacy_admitted_columns());
    }

    dataSource->OmittedInaccessibleColumns() = FromProto<std::vector<TString>>(protoDataSource.omitted_inaccessible_columns());

    if (protoDataSource.has_path()) {
        dataSource->SetPath(protoDataSource.path());
    }

    if (protoDataSource.has_timestamp()) {
        dataSource->SetTimestamp(protoDataSource.timestamp());
    }

    if (protoDataSource.has_retention_timestamp()) {
        dataSource->SetRetentionTimestamp(protoDataSource.retention_timestamp());
    }

    dataSource->SetForeign(protoDataSource.foreign());

    dataSource->ColumnRenameDescriptors() = FromProto<TColumnRenameDescriptors>(protoDataSource.column_rename_descriptors());
}

TDataSource MakeVersionedDataSource(
    const std::optional<TYPath>& path,
    const TTableSchema& schema,
    const std::optional<std::vector<TString>>& columns,
    const std::vector<TString>& omittedInaccessibleColumns,
    NTransactionClient::TTimestamp timestamp,
    NTransactionClient::TTimestamp retentionTimestamp,
    const TColumnRenameDescriptors& columnRenameDescriptors)
{
    return TDataSource(
        EDataSourceType::VersionedTable,
        path,
        schema,
        columns,
        omittedInaccessibleColumns,
        timestamp,
        retentionTimestamp,
        columnRenameDescriptors);
}

TDataSource MakeUnversionedDataSource(
    const std::optional<TYPath>& path,
    const std::optional<TTableSchema>& schema,
    const std::optional<std::vector<TString>>& columns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnRenameDescriptors& columnRenameDescriptors)
{
    return TDataSource(
        EDataSourceType::UnversionedTable,
        path,
        schema,
        columns,
        omittedInaccessibleColumns,
        NullTimestamp,
        NullTimestamp,
        columnRenameDescriptors);
}

TDataSource MakeFileDataSource(const std::optional<TYPath>& path)
{
    return TDataSource(
        EDataSourceType::File,
        path,
        std::nullopt,
        std::nullopt,
        {},
        NullTimestamp,
        NullTimestamp,
        {});
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDataSourceDirectoryExt* protoDataSourceDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory)
{
    using NYT::ToProto;
    TSchemaDictionary schemaDictionary;
    TColumnFilterDictionary columnFilterDictionary;
    for (const auto& dataSource : dataSourceDirectory->DataSources()) {
        auto* protoDataSource = protoDataSourceDirectory->add_data_sources();
        ToProto(protoDataSource, dataSource, &schemaDictionary, &columnFilterDictionary);
    }
    ToProto(protoDataSourceDirectory->mutable_schema_dictionary(), schemaDictionary);
    ToProto(protoDataSourceDirectory->mutable_column_filter_dictionary(), columnFilterDictionary);
}

void FromProto(
    TDataSourceDirectoryPtr* dataSourceDirectory,
    const NProto::TDataSourceDirectoryExt& protoDataSourceDirectory)
{
    using NYT::FromProto;
    *dataSourceDirectory = New<TDataSourceDirectory>();
    auto& dataSources = (*dataSourceDirectory)->DataSources();

    TSchemaDictionary schemaDictionary;
    FromProto(&schemaDictionary, protoDataSourceDirectory.schema_dictionary());

    TColumnFilterDictionary columnFilterDictionary;
    FromProto(&columnFilterDictionary, protoDataSourceDirectory.column_filter_dictionary());

    for (const auto& protoDataSource : protoDataSourceDirectory.data_sources()) {
        TDataSource dataSource;
        FromProto(&dataSource, protoDataSource, &schemaDictionary, &columnFilterDictionary);
        dataSources.emplace_back(std::move(dataSource));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

