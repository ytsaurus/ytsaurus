#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/misc/enum.h>

#include <optional>

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
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TObjectId, ObjectId);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TTableSchemaPtr, Schema);
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::vector<TString>>, Columns);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TString>, OmittedInaccessibleColumns);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, Timestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetentionTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TColumnRenameDescriptors, ColumnRenameDescriptors);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TVirtualValueDirectoryPtr, VirtualValueDirectory);
    DEFINE_BYVAL_RW_PROPERTY(int, VirtualKeyPrefixLength, 0);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, Account);

    //! Returns comparator built from data source schema. Crashes in case if data source is not sorted.
    NTableClient::TComparator GetComparator() const;

    TDataSource() = default;
    TDataSource(
        EDataSourceType type,
        const std::optional<NYPath::TYPath>& path,
        NTableClient::TTableSchemaPtr schema,
        int virtualKeyPrefixLength,
        const std::optional<std::vector<TString>>& columns,
        const std::vector<TString>& omittedInaccessibleColumns,
        NTransactionClient::TTimestamp timestamp,
        NTransactionClient::TTimestamp retentionTimestamp,
        const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors);
};

TDataSource MakeVersionedDataSource(
    const std::optional<NYPath::TYPath>& path,
    NTableClient::TTableSchemaPtr schema,
    const std::optional<std::vector<TString>>& columns,
    const std::vector<TString>& omittedInaccessibleColumns,
    NTransactionClient::TTimestamp timestamp,
    NTransactionClient::TTimestamp retentionTimestamp = NTransactionClient::NullTimestamp,
    const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors = {});

TDataSource MakeUnversionedDataSource(
    const std::optional<NYPath::TYPath>& path,
    NTableClient::TTableSchemaPtr schema,
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

    //! Get common data source type or throw if types are mixed.
    EDataSourceType GetCommonTypeOrThrow() const;
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
