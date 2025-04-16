#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TLockableDynamicTable
{
    NTableClient::TTableId TableId;
    NObjectClient::TTransactionId ExternalTransactionId;
};

////////////////////////////////////////////////////////////////////////////////

void LockDynamicTables(
    THashMap<NObjectClient::TCellTag, std::vector<TLockableDynamicTable>> lockableDynamicTables,
    const NApi::NNative::IConnectionPtr& connection,
    const TExponentialBackoffOptions& config,
    const NLogging::TLogger& Logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NTabletClient::NProto::TExternalCellTagToTableIds* protoExternalCellTagToTableIds,
    const std::pair<NObjectClient::TCellTag, std::vector<NTableClient::TTableId>>& externalCellTagToTableIds);

void FromProto(
    std::pair<NObjectClient::TCellTag, std::vector<NTableClient::TTableId>>* externalCellTagToTableIds,
    const NTabletClient::NProto::TExternalCellTagToTableIds& protoExternalCellTagToTableIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
