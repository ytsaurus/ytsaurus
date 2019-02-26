#pragma once

#include "private.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/table_client/public.h>

#include <yt/client/ypath/public.h>

#include <yt/core/logging/public.h>

#include <Core/Field.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TTableObject
    : public NChunkClient::TUserObject
{
    int ChunkCount = 0;
    bool Dynamic = false;
    NTableClient::TTableSchema Schema;
};

////////////////////////////////////////////////////////////////////////////////

DB::KeyCondition CreateKeyCondition(
    const DB::Context& context,
    const DB::SelectQueryInfo& queryInfo,
    const TClickHouseTableSchema& schema);

DB::Field ConvertToField(const NTableClient::TUnversionedValue& value);

void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, DB::Field* field);

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): unify with similar functions all over the code base.
std::unique_ptr<TTableObject> GetTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    NYTree::EPermission permission,
    const NLogging::TLogger& logger);

TClickHouseTablePtr FetchClickHouseTable(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

