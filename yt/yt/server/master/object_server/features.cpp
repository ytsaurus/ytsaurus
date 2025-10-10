#include "features.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/generic/algorithm.h>

namespace NYT::NObjectServer {

using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TTypeV3LogicalTypeWrapper> GetPrimitiveTypes()
{
    std::vector<TTypeV3LogicalTypeWrapper> result;
    for (auto type : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
        result.push_back(TTypeV3LogicalTypeWrapper{SimpleLogicalType(type)});
    }
    return result;
}

std::vector<NCompression::ECodec> GetCompressionCodecs(
    const std::optional<THashSet<NCompression::ECodec>>& configuredForbiddenCodecs)
{
    const auto& codecs = configuredForbiddenCodecs
        ? *configuredForbiddenCodecs
        : NCompression::GetForbiddenCodecs();
    std::vector<NCompression::ECodec> result;
    for (auto id : TEnumTraits<NCompression::ECodec>::GetDomainValues()) {
        if (!codecs.contains(id)) {
            result.push_back(id);
        }
    }
    SortUnique(result);
    return result;
}

std::vector<NErasure::ECodec> GetErasureCodecs()
{
    return NErasure::GetSupportedCodecIds();
}

std::vector<std::string> GetNodeFlavors()
{
    std::vector<std::string> result;
    for (auto value : TEnumTraits<ENodeFlavor>::GetDomainValues()) {
        result.push_back(std::string(Format("%lv", value)));
    }
    result.push_back(ClusterNodeFlavor);
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonString CreateFeatureRegistryYson(
    const std::optional<THashSet<NCompression::ECodec>>& configuredForbiddenCompressionCodecs)
{
    return BuildYsonStringFluently()
        .BeginMap()
            .Item("primitive_types").List(GetPrimitiveTypes())
            .Item("compression_codecs").List(GetCompressionCodecs(configuredForbiddenCompressionCodecs))
            .Item("erasure_codecs").List(GetErasureCodecs())
            .Item("query_memory_limit_in_tablet_nodes").Value(true)
            .Item("node_flavors").List(GetNodeFlavors())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

