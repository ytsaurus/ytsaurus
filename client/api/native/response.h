#pragma once

#include "public.h"

#include "payload.h"

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TAttributeList
{
    std::vector<TPayload> ValuePayloads;
    std::vector<TTimestamp> Timestamps;
};

void FromProto(
    TAttributeList* attributeList,
    const NProto::TAttributeList& protoAttributeList);

////////////////////////////////////////////////////////////////////////////////

struct TGenerateTimestampResult
{
    TTimestamp Timestamp;
};

struct TSelectObjectsResult
{
    std::vector<TAttributeList> Results;
    TTimestamp Timestamp;
    std::optional<TString> ContinuationToken;
};

struct TGetObjectResult
{
    TAttributeList Result;
    TTimestamp Timestamp;
};

////////////////////////////////////////////////////////////////////////////////

struct TUpdateObjectResult
{
    TTimestamp CommitTimestamp;
};

struct TCreateObjectResult
{
    TObjectId ObjectId;
    TTimestamp CommitTimestamp;
};

struct TRemoveObjectResult
{
    TTimestamp CommitTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionResult
{
    TTransactionId TransactionId;
    TTimestamp StartTimestamp;
};

struct TAbortTransactionResult
{ };

struct TCommitTransactionResult
{
    TTimestamp CommitTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

template <class... TAttributes>
void ParsePayloads(
    std::vector<TPayload> payloads,
    TAttributes*... attributes);

////////////////////////////////////////////////////////////////////////////////

void ValidatePayloadFormat(
    EPayloadFormat expectedFormat,
    const TPayload& payload);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative

#define RESPONSE_INL_H_
#include "response-inl.h"
#undef RESPONSE_INL_H_
