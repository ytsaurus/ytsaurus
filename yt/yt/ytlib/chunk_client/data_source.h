#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>
#include <yt/yt/ytlib/scheduler/input_query.h>

#include <yt/yt/library/query/row_level_security_api/row_level_security.h>

#include <yt/yt/client/table_client/column_rename_descriptor.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_io_options.h>

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

class TDataSource final
{
public:
    DEFINE_BYVAL_RW_PROPERTY(EDataSourceType, Type, EDataSourceType::UnversionedTable);
    DEFINE_BYVAL_RW_PROPERTY(bool, Foreign, false);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYPath::TYPath>, Path);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TObjectId, ObjectId);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TTableSchemaPtr, Schema);
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::vector<std::string>>, Columns);
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::string>, OmittedInaccessibleColumns);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, Timestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, RetentionTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TColumnRenameDescriptors, ColumnRenameDescriptors);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TVirtualValueDirectoryPtr, VirtualValueDirectory);
    DEFINE_BYVAL_RW_PROPERTY(int, VirtualKeyPrefixLength, 0);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<std::string>, Account);
    DEFINE_BYVAL_RW_PROPERTY(NScheduler::TClusterName, ClusterName);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NTableClient::TRlsReadSpec>, RlsReadSpec);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NScheduler::TInputQuerySpec>, InputQuerySpec);

    //! Returns comparator built from data source schema. Crashes in case if data source is not sorted.
    NTableClient::TComparator GetComparator() const;

    TDataSource() = default;
    TDataSource(
        EDataSourceType type,
        const std::optional<NYPath::TYPath>& path,
        NTableClient::TTableSchemaPtr schema,
        int virtualKeyPrefixLength,
        const std::optional<std::vector<std::string>>& columns,
        const std::vector<std::string>& omittedInaccessibleColumns,
        NTransactionClient::TTimestamp timestamp,
        NTransactionClient::TTimestamp retentionTimestamp,
        const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors);
};

DEFINE_REFCOUNTED_TYPE(TDataSource)

TDataSourcePtr MakeVersionedDataSource(
    const std::optional<NYPath::TYPath>& path,
    NTableClient::TTableSchemaPtr schema,
    const std::optional<std::vector<std::string>>& columns,
    const std::vector<std::string>& omittedInaccessibleColumns,
    NTransactionClient::TTimestamp timestamp,
    NTransactionClient::TTimestamp retentionTimestamp = NTransactionClient::NullTimestamp,
    const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors = {});

TDataSourcePtr MakeUnversionedDataSource(
    const std::optional<NYPath::TYPath>& path,
    NTableClient::TTableSchemaPtr schema,
    const std::optional<std::vector<std::string>>& columns,
    const std::vector<std::string>& omittedInaccessibleColumns,
    const NTableClient::TColumnRenameDescriptors& columnRenameDescriptors = {});

TDataSourcePtr MakeFileDataSource(const std::optional<NYPath::TYPath>& path);

void FromProto(
    TDataSourcePtr* dataSource,
    const NProto::TDataSource& protoDataSource,
    const NTableClient::TSchemaDictionary* schemaDictionary = nullptr,
    const NTableClient::TColumnFilterDictionary* columnFilterDictionary = nullptr);

void ToProto(
    NProto::TDataSource* protoDataSource,
    const TDataSourcePtr& dataSource,
    NTableClient::TSchemaDictionary* schemaDictionary = nullptr,
    NTableClient::TColumnFilterDictionary* columnFilterDictionary = nullptr);

////////////////////////////////////////////////////////////////////////////////

class TDataSourceDirectory
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TDataSourcePtr>, DataSources);

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
