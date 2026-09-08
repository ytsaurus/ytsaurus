#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/client/table_client/public.h>

#include <optional>

namespace NYql::NYtflow {

struct TOutputStreamInfo
{
    NYT::NFlow::TStreamId StreamId;
    NYT::NTableClient::TTableSchemaPtr OutputSchema;
};

std::optional<double> TryGetCpuToVCpuFactor();

} // namespace NYql::NYtflow
