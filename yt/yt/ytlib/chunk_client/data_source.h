#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/column_rename_descriptor.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/enum.h>

#include <yt/core/ytree/public.h>

namespace NYT::NChunkClient {

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
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYPath::TYPath>, Path);
    DEFINE_BYREF_RW_PROPERTY(std::optional<NTableClient::TTableSchema>, Schema);
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::vector<TString>>, Columns);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TString>, OmittedInaccessibleColumns);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, Timestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetentionTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TColumnRenameDescriptors, ColumnRenameDescriptors);

    TDataSource() = default;
    TDataSource(
        EDataSourceType type,
        const std::optional<NYPath::TYPath>& path,
        const std::optional<NTableClient::TTableSchema>& schema,
        const std::optional<std::vector<TString>>& columns,
        const std::vector<TString>& omittedInaccessibleColumns,
        NTransactionClient::TTimestamp timestamp,
        NTransactionClient::TTimestamp retentionTimestamp,
        const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors);
};

TDataSource MakeVersionedDataSource(
    const std::optional<NYPath::TYPath>& path,
    const NTableClient::TTableSchema& schema,
    const std::optional<std::vector<TString>>& columns,
    const std::vector<TString>& omittedInaccessibleColumns,
    NTransactionClient::TTimestamp timestamp,
    NTransactionClient::TTimestamp retentionTimestamp = NTransactionClient::NullTimestamp,
    const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors = {});

TDataSource MakeUnversionedDataSource(
    const std::optional<NYPath::TYPath>& path,
    const std::optional<NTableClient::TTableSchema>& schema,
    const std::optional<std::vector<TString>>& columns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors = {});

TDataSource MakeFileDataSource(const std::optional<NYPath::TYPath>& path);

void FromProto(
    TDataSource* dataSource,
    const NProto::TDataSource& protoDataSource,
    const NTableClient::TSchemaDictionary* schemaDictionary = nullptr,
    const NTableClient::TColumnFilterDictionary* columnFilterDictionary = nullptr);

void ToProto(
    NProto::TDataSource* protoDataSource,
    const TDataSource& dataSource,
    NTableClient::TSchemaDictionary* schemaDictionary = nullptr,
    NTableClient::TColumnFilterDictionary* columnFilterDictionary = nullptr);

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

} // namespace NYT::NChunkClient
