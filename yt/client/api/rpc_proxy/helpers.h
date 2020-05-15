#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>

#include <yt/library/re2/re2.h>

#include <yt/core/rpc/public.h>

#include <yt/client/api/admin.h>
#include <yt/client/api/client.h>

#include <yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void SetTimeoutOptions(
    NRpc::TClientRequest& request,
    const NApi::TTimeoutOptions& options);

[[noreturn]] void ThrowUnimplemented(const TString& method);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(
    NProto::TTransactionalOptions* proto,
    const NApi::TTransactionalOptions& options);

void ToProto(
    NProto::TPrerequisiteOptions* proto,
    const NApi::TPrerequisiteOptions& options);

void ToProto(
    NProto::TMasterReadOptions* proto,
    const NApi::TMasterReadOptions& options);

void ToProto(
    NProto::TMutatingOptions* proto,
    const NApi::TMutatingOptions& options);

void ToProto(
    NProto::TSuppressableAccessTrackingOptions* proto,
    const NApi::TSuppressableAccessTrackingOptions& options);

void ToProto(
    NProto::TTabletRangeOptions* proto,
    const NApi::TTabletRangeOptions& options);

void ToProto(
    NProto::TRetentionConfig* protoConfig,
    const NTableClient::TRetentionConfig& config);

void FromProto(
    NTableClient::TRetentionConfig* config,
    const NProto::TRetentionConfig& protoConfig);

void ToProto(
    NProto::TGetFileFromCacheResult* proto,
    const NApi::TGetFileFromCacheResult& result);

void FromProto(
    NApi::TGetFileFromCacheResult* result,
    const NProto::TGetFileFromCacheResult& proto);

void ToProto(
    NProto::TPutFileToCacheResult* proto,
    const NApi::TPutFileToCacheResult& result);

void FromProto(
    NApi::TPutFileToCacheResult* result,
    const NProto::TPutFileToCacheResult& proto);

void ToProto(
    NProto::TCheckPermissionResult* proto,
    const NApi::TCheckPermissionResult& result);

void FromProto(
    NApi::TCheckPermissionResult* result,
    const NProto::TCheckPermissionResult& proto);

void ToProto(
    NProto::TCheckPermissionByAclResult* proto,
    const NApi::TCheckPermissionByAclResult& result);

void FromProto(
    NApi::TCheckPermissionByAclResult* result,
    const NProto::TCheckPermissionByAclResult& proto);

void ToProto(
    NProto::TListOperationsResult* proto,
    const NApi::TListOperationsResult& result);

void FromProto(
    NApi::TListOperationsResult* result,
    const NProto::TListOperationsResult& proto);

void ToProto(
    NProto::TListJobsResult* proto,
    const NApi::TListJobsResult& result);

void FromProto(
    NApi::TListJobsResult* result,
    const NProto::TListJobsResult& proto);

void ToProto(NProto::TColumnSchema* protoSchema, const NTableClient::TColumnSchema& schema);
void FromProto(NTableClient::TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

void ToProto(NProto::TTableSchema* protoSchema, const NTableClient::TTableSchema& schema);
void FromProto(NTableClient::TTableSchema* schema, const NProto::TTableSchema& protoSchema);

// Doesn't fill cell_config_version.
void ToProto(
    NProto::TTabletInfo* protoTabletInfo,
    const NTabletClient::TTabletInfo& tabletInfo);
// Doesn't fill TableId, UpdateTime and Owners.
void FromProto(
    NTabletClient::TTabletInfo* tabletInfo,
    const NProto::TTabletInfo& protoTabletInfo);

void ToProto(
    NProto::TTabletReadOptions* protoOptions,
    const NApi::TTabletReadOptions& options);

void ToProto(
    NProto::TQueryStatistics* protoStatistics,
    const NQueryClient::TQueryStatistics& statistics);

void FromProto(
    NQueryClient::TQueryStatistics* statistics,
    const NProto::TQueryStatistics& protoStatistics);

void ToProto(
    NProto::TOperation* protoOperation,
    const NApi::TOperation& operation);

void FromProto(
    NApi::TOperation* operation,
    const NProto::TOperation& protoOperation);

void ToProto(
    NProto::TJob* protoJob,
    const NApi::TJob& job);

void FromProto(
    NApi::TJob* job,
    const NProto::TJob& protoJob);

void ToProto(
    NProto::TListJobsStatistics* protoStatistics,
    const NApi::TListJobsStatistics& statistics);

void FromProto(
    NApi::TListJobsStatistics* statistics,
    const NProto::TListJobsStatistics& protoStatistics);

void ToProto(
    NProto::TColumnarStatistics* protoStatistics,
    const NTableClient::TColumnarStatistics& statistics);

void FromProto(
    NTableClient::TColumnarStatistics* statistics,
    const NProto::TColumnarStatistics& protoStatistics);

template <class TStringContainer>
void ToProto(
    NRpcProxy::NProto::TAttributeKeys* protoAttributes,
    const std::optional<TStringContainer>& attributes);

NProto::EOperationType ConvertOperationTypeToProto(
    const NScheduler::EOperationType& operationType);

NScheduler::EOperationType ConvertOperationTypeFromProto(
    const NProto::EOperationType& proto);

NProto::EOperationState ConvertOperationStateToProto(
    const NScheduler::EOperationState& operationState);

NScheduler::EOperationState ConvertOperationStateFromProto(
    const NProto::EOperationState& proto);

NProto::EJobType ConvertJobTypeToProto(
    const NJobTrackerClient::EJobType& jobType);

NJobTrackerClient::EJobType ConvertJobTypeFromProto(
    const NProto::EJobType& proto);

NProto::EJobState ConvertJobStateToProto(
    const NJobTrackerClient::EJobState& jobState);

NJobTrackerClient::EJobState ConvertJobStateFromProto(
    const NProto::EJobState& proto);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error, bool retryProxyBanned = true);

////////////////////////////////////////////////////////////////////////////////

void ValidateRowsetDescriptor(
    const NProto::TRowsetDescriptor& descriptor,
    int expectedVersion,
    NProto::ERowsetKind expectedKind);

std::vector<TSharedRef> SerializeRowsetWithPartialNameTable(
    const NTableClient::TNameTablePtr& nameTable,
    int startingId,
    TRange<NTableClient::TUnversionedRow> rows,
    NProto::TRowsetDescriptor* descriptor);

std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TNameTablePtr& nameTable,
    TRange<NTableClient::TUnversionedRow> rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TTableSchema& schema,
    TRange<TRow> rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
TIntrusivePtr<NApi::IRowset<TRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);

//! Serializes an unversioned rowset and a name table except for the first
//! #nameTableSize names in it, then updates #nameTableSize. #nameTableSize
//! needs to be tracked to accommodate for potential simultaneous name table
//! updates.
TSharedRef SerializeRowsetWithNameTableDelta(
    const NTableClient::TNameTablePtr& nameTable,
    TRange<NTableClient::TUnversionedRow> rows,
    int* nameTableSize);

//! Deserializes an unversioned rowset and new columns for the #nameTable,
//! updates the #nameTable and #descriptor. If #idMapping is specified, it is
//! applied to the rowset and updated, too.
TSharedRange<NTableClient::TUnversionedRow> DeserializeRowsetWithNameTableDelta(
    const TSharedRef& data,
    const NTableClient::TNameTablePtr& nameTable,
    NProto::TRowsetDescriptor* descriptor,
    NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr);

////////////////////////////////////////////////////////////////////////////////

//! Invokes std::stable_sort reordering addesses by the index of the first regex they match;
//! addresses not matching any regex are placed at the very end.
void SortByRegexes(std::vector<TString>& values, const std::vector<NRe2::TRe2Ptr>& regexes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
