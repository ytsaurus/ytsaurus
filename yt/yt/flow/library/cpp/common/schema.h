#pragma once

#include "payload_converter.h"
#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/query/engine_api/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ValidateStreamSchema(const NTableClient::TTableSchema& schema);
void ValidateIsGroupable(
    const NTableClient::TTableSchema& schema,
    const NTableClient::TTableSchema& groupBySchema);
void ValidateSchemaExpressions(
    const NTableClient::TTableSchemaPtr& schema,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache = nullptr);
void ValidateGroupBySchema(
    const NTableClient::TTableSchemaPtr& schema,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache = nullptr);

////////////////////////////////////////////////////////////////////////////////

TMessage ConvertMessageToNewSchema(
    const TMessage& message,
    const NTableClient::TTableSchemaPtr& targetSchema,
    const IPayloadConverterCachePtr& converterCache);

TPayload ConvertPayloadToNewSchema(
    const TPayload& payload,
    const NTableClient::TTableSchemaPtr& sourceSchema,
    const NTableClient::TTableSchemaPtr& targetSchema,
    const IPayloadConverterCachePtr& converterCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
