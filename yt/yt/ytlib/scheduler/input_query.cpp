#include "input_query.h"
#include "config.h"

#include <yt/yt/ytlib/scheduler/proto/input_query.pb.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NScheduler {

using NYT::FromProto;
using NYT::ToProto;
using NYT::Load;
using NYT::Save;

////////////////////////////////////////////////////////////////////////////////

bool TInputQuerySpec::CanCreateGranuleFilter() const
{
    return Query->WhereClause && QueryFilterOptions->EnableChunkFilter;
}

////////////////////////////////////////////////////////////////////////////////

void TInputQuerySpecSerializer::Save(TStreamSaveContext& context, const std::optional<TInputQuerySpec>& querySpec)
{
    Save<bool>(context, querySpec.has_value());
    if (querySpec.has_value()) {
        auto protoQuerySpec = ToProto<NProto::TQuerySpec>(*querySpec);
        Save<NProto::TQuerySpec>(context, protoQuerySpec);
    }
}

void TInputQuerySpecSerializer::Load(TStreamLoadContext& context, std::optional<TInputQuerySpec>& querySpec)
{
    bool hasValue = Load<bool>(context);
    if (hasValue) {
        auto protoQuerySpec = Load<NProto::TQuerySpec>(context);
        querySpec.emplace(FromProto<TInputQuerySpec>(protoQuerySpec));
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TQuerySpec* protoQuerySpec,
    const TInputQuerySpec& querySpec)
{
    NQueryClient::ToProto(protoQuerySpec->mutable_query(), querySpec.Query);
    protoQuerySpec->mutable_query()->set_input_row_limit(std::numeric_limits<i64>::max() / 4);
    protoQuerySpec->mutable_query()->set_output_row_limit(std::numeric_limits<i64>::max() / 4);
    ToProto(protoQuerySpec->mutable_options(), *querySpec.QueryFilterOptions);
    ToProto(protoQuerySpec->mutable_external_functions(), querySpec.ExternalCGInfo->Functions);
}

void FromProto(
    TInputQuerySpec* querySpec,
    const NProto::TQuerySpec& protoQuerySpec)
{
    NQueryClient::FromProto(&querySpec->Query, protoQuerySpec.query());
    querySpec->QueryFilterOptions = New<TInputQueryFilterOptions>();
    FromProto(querySpec->QueryFilterOptions.Get(), protoQuerySpec.options());
    querySpec->ExternalCGInfo = New<NQueryClient::TExternalCGInfo>();
    FromProto(&querySpec->ExternalCGInfo->Functions, protoQuerySpec.external_functions());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
