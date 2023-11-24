#include "virtual.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NControllerAgent {

using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TVirtualStaticTable::TVirtualStaticTable(
    const THashSet<NChunkClient::TInputChunkPtr>& chunks,
    TTableSchemaPtr schema,
    TNodeDirectoryPtr nodeDirectory)
    : Chunks_(chunks)
    , Schema_(std::move(schema))
    , NodeDirectory_(std::move(nodeDirectory))
{ }

bool TVirtualStaticTable::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(GetBasicAttributes);
    // TODO(max42): Or DISPATCH_YPATH_HEAVY_SERVICE_METHOD(Fetch)?
    DISPATCH_YPATH_SERVICE_METHOD(Fetch);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    return TSupportsAttributes::DoInvoke(context);
}

DEFINE_YPATH_SERVICE_METHOD(TVirtualStaticTable, GetBasicAttributes)
{
    if (request->has_permission()) {
        auto permission = CheckedEnumCast<EPermission>(request->permission());
        ValidatePermission(EPermissionCheckScope::This, permission);
    }

    ToProto(response->mutable_object_id(), TGuid());
    response->set_external_cell_tag(ToProto<int>(PrimaryMasterCellTagSentinel));

    context->SetResponseInfo();
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TVirtualStaticTable, Fetch)
{
    TNodeDirectoryBuilder nodeDirectoryBuilder(NodeDirectory_, response->mutable_node_directory());

    for (const auto& range : FromProto<std::vector<TLegacyReadRange>>(request->ranges())) {
        auto lowerLimit = range.LowerLimit();
        auto upperLimit = range.UpperLimit();
        if (lowerLimit.HasLegacyKey() || lowerLimit.HasOffset() || upperLimit.HasLegacyKey() || upperLimit.HasOffset()) {
            THROW_ERROR_EXCEPTION("Only row indices and chunk indices are supported as read limits in virtual static table");
        }
        auto lowerLimitRowIndex = lowerLimit.HasRowIndex() ? lowerLimit.GetRowIndex() : 0;
        auto upperLimitRowIndex = upperLimit.HasRowIndex() ? upperLimit.GetRowIndex() : std::numeric_limits<i64>::max() / 4;
        auto lowerLimitChunkIndex = lowerLimit.HasChunkIndex() ? lowerLimit.GetChunkIndex() : 0;
        auto upperLimitChunkIndex = upperLimit.HasChunkIndex() ? upperLimit.GetChunkIndex() : std::numeric_limits<i32>::max() / 4;
        i64 tableRowIndex = 0;
        int chunkIndex = 0;
        for (auto chunkIt = Chunks_.begin(); chunkIt != Chunks_.end(); ++chunkIt, ++chunkIndex) {
            auto chunk = *chunkIt;
            auto lowerTableRowIndex = tableRowIndex;
            auto upperTableRowIndex = tableRowIndex + chunk->GetRowCount();
            tableRowIndex += chunk->GetRowCount();
            if (upperTableRowIndex <= lowerLimitRowIndex || lowerTableRowIndex >= upperLimitRowIndex) {
                continue;
            }
            if (chunkIndex < lowerLimitChunkIndex || chunkIndex >= upperLimitChunkIndex) {
                continue;
            }
            auto chunkLowerLimit = std::max<i64>(lowerLimitRowIndex - lowerTableRowIndex, 0);
            auto chunkUpperLimit = std::min<i64>(upperLimitRowIndex - lowerTableRowIndex, chunk->GetRowCount());
            auto* chunkSpec = response->add_chunks();
            ToProto(chunkSpec, chunk);
            // NB: chunk we got may have non-zero table index, override it with zero.
            chunkSpec->set_table_index(0);
            nodeDirectoryBuilder.Add(chunk->GetReplicaList());
            chunkSpec->set_row_count_override(chunkUpperLimit - chunkLowerLimit);
            if (chunkLowerLimit != 0) {
                chunkSpec->mutable_lower_limit()->set_row_index(chunkLowerLimit);
            }
            if (chunkUpperLimit != chunk->GetRowCount()) {
                chunkSpec->mutable_upper_limit()->set_row_index(chunkUpperLimit);
            }
        }
    }

    context->SetResponseInfo();
    context->Reply();
}

void TVirtualStaticTable::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back(EInternedAttributeKey::Schema);
    descriptors->push_back(EInternedAttributeKey::Sorted);
    if (Schema_->IsSorted()) {
        descriptors->push_back(EInternedAttributeKey::SortedBy);
    }
    descriptors->push_back(EInternedAttributeKey::SchemaMode);
    descriptors->push_back(EInternedAttributeKey::ChunkCount);
    descriptors->push_back(EInternedAttributeKey::Dynamic);
    descriptors->push_back(EInternedAttributeKey::Type);
}

const THashSet<TInternedAttributeKey>& TVirtualStaticTable::GetBuiltinAttributeKeys()
{
    return BuiltinAttributeKeysCache_.GetBuiltinAttributeKeys(this);
}

bool TVirtualStaticTable::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    switch (key) {
        case EInternedAttributeKey::Schema:
            BuildYsonFluently(consumer)
                .Value(Schema_);
            return true;
        case EInternedAttributeKey::Sorted:
            BuildYsonFluently(consumer)
                .Value(Schema_->IsSorted());
            return true;
        case EInternedAttributeKey::SortedBy:
            if (Schema_->IsSorted()) {
                BuildYsonFluently(consumer)
                    .Value(Schema_->GetKeyColumns());
                return true;
            }
            return false;
        case EInternedAttributeKey::SchemaMode:
            BuildYsonFluently(consumer)
                .Value(ETableSchemaMode::Weak);
            return true;
        case EInternedAttributeKey::ChunkCount:
            BuildYsonFluently(consumer)
                .Value(Chunks_.size());
            return true;
        case EInternedAttributeKey::Dynamic:
            BuildYsonFluently(consumer)
                .Value(false);
            return true;
        case EInternedAttributeKey::Type:
            BuildYsonFluently(consumer)
                .Value(EObjectType::Table);
            return true;
        default:
            return false;
    }
}

TFuture<TYsonString> TVirtualStaticTable::GetBuiltinAttributeAsync(TInternedAttributeKey /*key*/)
{
    return std::nullopt;
}

ISystemAttributeProvider* TVirtualStaticTable::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualStaticTable::SetBuiltinAttribute(TInternedAttributeKey /*key*/, const TYsonString& /*value*/, bool /*force*/)
{
    return false;
}

bool TVirtualStaticTable::RemoveBuiltinAttribute(TInternedAttributeKey /*key*/)
{
    return false;
}

void TVirtualStaticTable::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    TAsyncYsonWriter writer;

    if (request->has_attributes()) {
        writer.OnBeginAttributes();
        auto attributeFilter = FromProto<TAttributeFilter>(request->attributes());
        DoWriteAttributesFragment(&writer, attributeFilter, false /*stable*/);
        writer.OnEndAttributes();
    }
    writer.OnEntity();

    writer.Finish()
        .Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
            if (resultOrError.IsOK()) {
                response->set_value(resultOrError.Value().ToString());
                context->Reply();
            } else {
                context->Reply(resultOrError);
            }
        }));
}

void TVirtualStaticTable::DoWriteAttributesFragment(
    NYson::IAsyncYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    bool /*stable*/)
{
    if (!attributeFilter) {
        return;
    }
    attributeFilter.ValidateKeysOnly("virtual static table");
    auto builtinAttributeKeys = GetBuiltinAttributeKeys();
    BuildYsonMapFragmentFluently(consumer)
        .DoFor(attributeFilter.Keys, [&] (TFluentMap fluent, const TString& key) {
            auto internedKey = TInternedAttributeKey::Lookup(key);
            if (builtinAttributeKeys.contains(internedKey)) {
                fluent
                    .Item(key);
                YT_VERIFY(GetBuiltinAttribute(internedKey, fluent.GetConsumer()));
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

