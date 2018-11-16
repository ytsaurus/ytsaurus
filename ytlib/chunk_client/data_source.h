#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/column_rename_descriptor.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/enum.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDataSourceType,
    ((File)                 (0))
    ((UnversionedTable)     (1))
    ((VersionedTable)       (2))
);

class TDataSource
{
public:
    DEFINE_BYVAL_RW_PROPERTY(EDataSourceType, Type, EDataSourceType::UnversionedTable);
    DEFINE_BYVAL_RW_PROPERTY(bool, Foreign, false);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TString>, Path);
    DEFINE_BYREF_RW_PROPERTY(TNullable<NTableClient::TTableSchema>, Schema);
    DEFINE_BYREF_RW_PROPERTY(TNullable<std::vector<TString>>, Columns);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, Timestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TColumnRenameDescriptors, ColumnRenameDescriptors);

    TDataSource() = default;

    TDataSource(
        EDataSourceType type,
        const TNullable<TString>& path,
        const TNullable<NTableClient::TTableSchema>& schema,
        const TNullable<std::vector<TString>>& columns,
        NTransactionClient::TTimestamp timestamp,
        const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors);
};

TDataSource MakeVersionedDataSource(
    const TNullable<TString>& path,
    const NTableClient::TTableSchema& schema,
    const TNullable<std::vector<TString>>& columns,
    NTransactionClient::TTimestamp timestamp,
    const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors = {});

TDataSource MakeUnversionedDataSource(
    const TNullable<TString>& path,
    const TNullable<NTableClient::TTableSchema>& schema,
    const TNullable<std::vector<TString>>& columns,
    const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors = {});

TDataSource MakeFileDataSource(const TNullable<TString>& path);

void FromProto(
    TDataSource* dataSource,
    const NProto::TDataSource& protoDataSource,
    const NTableClient::TSchemaDictionary* dictionary = nullptr);

void ToProto(
    NProto::TDataSource* protoDataSource,
    const TDataSource& dataSource,
    NTableClient::TSchemaDictionary* dictionary = nullptr);

////////////////////////////////////////////////////////////////////////////////

class TDataSourceDirectory
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TDataSource>, DataSources);
};

DEFINE_REFCOUNTED_TYPE(TDataSourceDirectory)

void ToProto(
    NProto::TDataSourceDirectoryExt* protoDataSourceDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory);

void FromProto(
    TDataSourceDirectoryPtr* dataSourceDirectory,
    const NProto::TDataSourceDirectoryExt& protoDataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
