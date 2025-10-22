#pragma once

#include "public.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TInputQuerySpec
{
    NQueryClient::TConstQueryPtr Query;
    NQueryClient::TExternalCGInfoPtr ExternalCGInfo;
    TInputQueryFilterOptionsPtr QueryFilterOptions;

    bool CanCreateGranuleFilter() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Implements phoenix serializations via protobuf.
struct TInputQuerySpecSerializer
{
    //! Serializes a given object into a given stream using ToProto function.
    //! Throws an exception in case of error.
    static void Save(TStreamSaveContext& context, const std::optional<TInputQuerySpec>& querySpec);

    //! Reads from a given stream protobuf message and convert using FromProto function.
    //! Throws an exception in case of error.
    static void Load(TStreamLoadContext& context, std::optional<TInputQuerySpec>& querySpec);
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TQuerySpec* protoQuerySpec,
    const TInputQuerySpec& querySpec);

void FromProto(
    TInputQuerySpec* querySpec,
    const NProto::TQuerySpec& protoQuerySpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
