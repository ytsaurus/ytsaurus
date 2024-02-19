#pragma once

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>
#include <yt/yt/library/query/engine_api/position_independent_value.h>

#include <yt/yt/library/web_assembly/api/data_transfer.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

template <>
TCopyGuard CopyIntoCompartment(TRange<NQueryClient::TPIValue> range, IWebAssemblyCompartment* compartment);

////////////////////////////////////////////////////////////////////////////////

TCopyGuard CopyOpaqueDataIntoCompartment(
    TRange<void*> opaqueData,
    TRange<size_t> opaqueDataSizes,
    IWebAssemblyCompartment* compartment);

std::pair<TCopyGuard, std::vector<NQueryClient::TPIValue*>> CopyRowRangeIntoCompartment(
    std::vector<const NQueryClient::TValue*>& rows,
    i64 stringLikeColumnsDataWeight,
    const NQueryClient::TRowSchemaInformation& rowSchemaInformation,
    IWebAssemblyCompartment* compartment);

std::pair<TCopyGuard, std::vector<NQueryClient::TPIValue*>> CopyRowRangeIntoCompartment(
    TRange<TRange<NQueryClient::TPIValue>> rows,
    IWebAssemblyCompartment* compartment);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
