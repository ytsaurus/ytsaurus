#include "data_source.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NYTree;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////_

TDataSource::TDataSource(
    EDataSourceType type,
    const TNullable<Stroka>& path,
    const TNullable<TTableSchema>& schema,
    TTimestamp timestamp)
    : Type_(type)
    , Path_(path)
    , Schema_(schema)
    , Timestamp_(timestamp)
{ }

void ToProto(NProto::TDataSource* protoDataSource, const TDataSource& dataSource)
{
    protoDataSource->set_type(static_cast<int>(dataSource.GetType()));

    if (dataSource.Schema()) {
        ToProto(protoDataSource->mutable_table_schema(), *dataSource.Schema());
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

    if (protoDataSource.has_path()) {
        dataSource->Path_ = protoDataSource.path();
    }

    if (protoDataSource.has_timestamp()) {
        dataSource->Timestamp_ = protoDataSource.timestamp();
    }
}

TDataSource MakeVersionedDataSource(
    const TNullable<Stroka>& path,
    const NTableClient::TTableSchema& schema,
    NTransactionClient::TTimestamp timestamp)
{
    return TDataSource(EDataSourceType::VersionedTable, path, schema, timestamp);
}

TDataSource MakeUnversionedDataSource(
    const TNullable<Stroka>& path,
    const TNullable<NTableClient::TTableSchema>& schema)
{
    return TDataSource(EDataSourceType::UnversionedTable, path, schema, NullTimestamp);
}

TDataSource MakeFileDataSource(const TNullable<Stroka>& path)
{
    return TDataSource(EDataSourceType::File, path, Null, NullTimestamp);
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

