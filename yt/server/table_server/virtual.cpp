#include "virtual.h"

#include <yt/server/misc/interned_attributes.h>

#include <yt/ytlib/chunk_client/input_chunk.h>
#include <yt/ytlib/chunk_client/data_source.h>

#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/async_writer.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableServer {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TVirtualStaticTable::TVirtualStaticTable(const THashSet<NYT::NChunkClient::TInputChunkPtr>& chunks, TNodeDirectoryPtr nodeDirectory)
    : Chunks_(chunks)
    , NodeDirectory_(std::move(nodeDirectory))
{ }

bool TVirtualStaticTable::DoInvoke(const IServiceContextPtr& context)
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
    auto permissions = EPermissionSet(request->permissions());
    for (auto permission : TEnumTraits<EPermission>::GetDomainValues()) {
        if (Any(permissions & permission)) {
            ValidatePermission(EPermissionCheckScope::This, permission);
        }
    }

    ToProto(response->mutable_object_id(), TGuid());
    response->set_cell_tag(PrimaryMasterCellTag);

    context->SetResponseInfo();
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TVirtualStaticTable, Fetch)
{
    TNodeDirectoryBuilder nodeDirectoryBuilder(NodeDirectory_, response->mutable_node_directory());

    for (const auto& range : FromProto<std::vector<TReadRange>>(request->ranges())) {
        auto lowerLimit = range.LowerLimit();
        auto upperLimit = range.UpperLimit();
        if (lowerLimit.HasKey() || lowerLimit.HasOffset() || upperLimit.HasKey() || upperLimit.HasOffset()) {
            THROW_ERROR_EXCEPTION("Only row indices and chunk indices are supported as read limits in virtual static table");
        }
        auto lowerLimitRowIndex = lowerLimit.HasRowIndex() ? lowerLimit.GetRowIndex() : 0;
        auto upperLimitRowIndex = upperLimit.HasRowIndex() ? upperLimit.GetRowIndex() : std::numeric_limits<i64>::max();
        auto lowerLimitChunkIndex = lowerLimit.HasChunkIndex() ? lowerLimit.GetChunkIndex() : 0;
        auto upperLimitChunkIndex = upperLimit.HasChunkIndex() ? upperLimit.GetChunkIndex() : std::numeric_limits<i32>::max();
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
            ToProto(chunkSpec, chunk, EDataSourceType::UnversionedTable);
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
                .Value(TTableSchema());
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
    return Null;
}

ISystemAttributeProvider* TVirtualStaticTable::GetBuiltinAttributeProvider()
{
    return this;
}

bool TVirtualStaticTable::SetBuiltinAttribute(TInternedAttributeKey /*key*/, const TYsonString& /*value*/)
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
        auto keys = NYT::FromProto<std::vector<TString>>(request->attributes().keys());
        writer.OnBeginAttributes();
        DoWriteAttributesFragment(&writer, MakeNullable(keys), false /* stable */);
        writer.OnEndAttributes();
    }
    writer.OnEntity();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().GetData());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TVirtualStaticTable::DoWriteAttributesFragment(
    NYT::NYson::IAsyncYsonConsumer* consumer,
    const NYT::TNullable<std::vector<TString>>& attributeKeys,
    bool /* stable */)
{
    if (!attributeKeys) {
        return;
    }
    auto builtinAttributeKeys = GetBuiltinAttributeKeys();
    BuildYsonMapFluently(consumer)
        .DoFor(*attributeKeys, [&] (TFluentMap fluent, const TString& key) {
            auto internedKey = GetInternedAttributeKey(key);
            if (builtinAttributeKeys.has(internedKey)) {
                fluent
                    .Item(key);
                YCHECK(GetBuiltinAttribute(internedKey, fluent.GetConsumer()));
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

