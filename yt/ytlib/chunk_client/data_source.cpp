#include "data_source.h"

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

void ToProto(NProto::TDataSource* protoDataSource, const TDataSource& dataSource)
{
    using NYT::ToProto;

    protoDataSource->set_type(static_cast<int>(dataSource.GetType()));

    if (dataSource.Schema()) {
        ToProto(protoDataSource->mutable_table_schema(), *dataSource.Schema());
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

void FromProto(TDataSource* dataSource, const NProto::TDataSource& protoDataSource)
{
    using NYT::FromProto;

    dataSource->Type_ = EDataSourceType(protoDataSource.type());

    if (protoDataSource.has_table_schema()) {
        dataSource->Schema_ = FromProto<TTableSchema>(protoDataSource.table_schema());
    }

    if (protoDataSource.has_column_filter()) {
        dataSource->Columns_ = FromProto<std::vector<TString>>(protoDataSource.columns());
    }

    if (protoDataSource.has_path()) {
        dataSource->Path_ = protoDataSource.path();
    }

    if (protoDataSource.has_timestamp()) {
        dataSource->Timestamp_ = protoDataSource.timestamp();
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
    NProto::TDataSourceDirectory* protoDataSourceDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory)
{
    using NYT::ToProto;
    ToProto(protoDataSourceDirectory->mutable_data_sources(), dataSourceDirectory->DataSources());
}

void FromProto(
    TDataSourceDirectoryPtr* dataSourceDirectory,
    const NProto::TDataSourceDirectory& protoDataSourceDirectory)
{
    using NYT::FromProto;
    *dataSourceDirectory = New<TDataSourceDirectory>();
    auto& dataSources = (*dataSourceDirectory)->DataSources();
    FromProto(&dataSources, protoDataSourceDirectory.data_sources());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

