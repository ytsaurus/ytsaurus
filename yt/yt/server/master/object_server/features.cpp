#include "features.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NObjectServer {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ListPrimitiveTypes(TFluentList fluent)
{
    for (auto type : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
        fluent.Item().Value(TTypeV3LogicalTypeWrapper{SimpleLogicalType(type)});
    }
}

void ListCompressionCodecs(TFluentList fluent, const THashSet<NCompression::ECodec>& configuredDeprecatedCodecIds)
{
    for (auto codecId : TEnumTraits<NCompression::ECodec>::GetDomainValues()) {
        if (configuredDeprecatedCodecIds.contains(codecId)) {
            continue;
        }
        fluent.Item().Value(codecId);
    }
}

void ListErasureCodecs(TFluentList fluent)
{
    static const THashSet<NErasure::ECodec> UniqueValues = {
        std::begin(TEnumTraits<NErasure::ECodec>::GetDomainValues()),
        std::end(TEnumTraits<NErasure::ECodec>::GetDomainValues())};
    for (auto codec : UniqueValues) {
        fluent.Item().Value(codec);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonString CreateFeatureRegistryYson(
    const std::optional<THashSet<NCompression::ECodec>>& configuredDeprecatedCodecIds)
{
    return BuildYsonStringFluently()
        .BeginMap()
            .Item("primitive_types").DoList(ListPrimitiveTypes)
            .Item("compression_codecs").DoList([&] (auto fluent) {
                ListCompressionCodecs(fluent, configuredDeprecatedCodecIds.value_or(NCompression::GetDeprecatedCodecIds()));
            })
            .Item("erasure_codecs").DoList(ListErasureCodecs)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

