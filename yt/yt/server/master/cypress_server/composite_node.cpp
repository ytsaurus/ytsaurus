#include "composite_node.h"

#include "helpers.h"

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>
#include <yt/yt/server/master/chaos_server/chaos_manager.h>

#include <yt/yt/server/master/tablet_server/tablet_manager.h>
#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NYson;
using namespace NYTree;
using namespace NCellMaster;
using namespace NChunkServer;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

template <bool Transient>
void TCompositeCypressNode::TAttributes<Transient>::Persist(const NCellMaster::TPersistenceContext& context)
    requires (!Transient)
{
    using NCellMaster::EMasterReign;
    using NYT::Persist;

    Persist(context, CompressionCodec);
    Persist(context, ErasureCodec);
    Persist(context, HunkErasureCodec);
    Persist(context, EnableStripedErasure);
    Persist(context, ReplicationFactor);
    Persist(context, Vital);
    Persist(context, Atomicity);
    Persist(context, CommitOrdering);
    Persist(context, InMemoryMode);
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Persist(context, OptimizeFor);
    } else {
        auto compatOptimizeFor = Load<TVersionedBuiltinAttribute<NTableClient::ECompatOptimizeFor>>(context.LoadContext());
        if (compatOptimizeFor.IsNull()) {
            OptimizeFor.Reset();
        } else if (compatOptimizeFor.IsTombstoned()) {
            OptimizeFor.Remove();
        } else if (compatOptimizeFor.IsSet()) {
            auto optimizeFor = compatOptimizeFor.ToOptional();
            YT_VERIFY(optimizeFor);
            OptimizeFor.Set(CheckedEnumCast<NTableClient::EOptimizeFor>(*optimizeFor));
        }
    }
    Persist(context, ProfilingMode);
    Persist(context, ProfilingTag);
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Persist(context, ChunkMergerMode);
    } else {
        auto compatChunkMergerMode = Load<TVersionedBuiltinAttribute<NChunkClient::ECompatChunkMergerMode>>(context.LoadContext());
        if (compatChunkMergerMode.IsNull()) {
            ChunkMergerMode.Reset();
        } else if (compatChunkMergerMode.IsTombstoned()) {
            ChunkMergerMode.Remove();
        } else if (compatChunkMergerMode.IsSet()) {
            auto chunkMergerMode = compatChunkMergerMode.ToOptional();
            YT_VERIFY(chunkMergerMode);
            ChunkMergerMode.Set(CheckedEnumCast<NChunkClient::EChunkMergerMode>(*chunkMergerMode));
        }
    }
    Persist(context, PrimaryMediumIndex);
    Persist(context, Media);
    Persist(context, TabletCellBundle);
    Persist(context, ChaosCellBundle);
    Persist(context, HunkMedia);
    Persist(context, HunkPrimaryMediumIndex);
}

template <bool Transient>
void TCompositeCypressNode::TAttributes<Transient>::Persist(const NCypressServer::TCopyPersistenceContext& context)
    requires Transient
{
    using NYT::Persist;
#define XX(camelCaseName, snakeCaseName) \
    Persist(context, camelCaseName);

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeCypressNode::TAttributes<Transient>::AreFull() const
{
#define XX(camelCaseName, snakeCaseName) \
    && camelCaseName.IsSet()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeCypressNode::TAttributes<Transient>::AreEmpty() const
{
#define XX(camelCaseName, snakeCaseName) \
    && !camelCaseName.IsNull()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
TCompositeCypressNode::TPersistentAttributes
TCompositeCypressNode::TAttributes<Transient>::ToPersistent() const
    requires Transient
{
    TPersistentAttributes result;
#define XX(camelCaseName, snakeCaseName) \
    if (camelCaseName.IsSet()) { \
        result.camelCaseName.Set(TVersionedBuiltinAttributeTraits<decltype(result.camelCaseName)::TValue>::FromRaw(camelCaseName.Unbox())); \
    }
    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX
    return result;
}

template <bool Transient>
TCompositeCypressNode::TAttributes<Transient>
TCompositeCypressNode::TAttributes<Transient>::Clone() const
    requires (!Transient)
{
    TCompositeCypressNode::TAttributes<Transient> result;
#define XX(camelCaseName, snakeCaseName) \
    result.camelCaseName = camelCaseName.Clone();
    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX
    return result;
}

template struct TCompositeCypressNode::TAttributes<true>;
template struct TCompositeCypressNode::TAttributes<false>;

////////////////////////////////////////////////////////////////////////////////

void TCompositeCypressNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    TUniquePtrSerializer<>::Save(context, Attributes_);
}

void TCompositeCypressNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    TUniquePtrSerializer<>::Load(context, Attributes_);
}

bool TCompositeCypressNode::HasInheritableAttributes() const
{
    auto* node = this;
    for (;;) {
        if (node->Attributes_) {
            YT_ASSERT(!node->Attributes_->AreEmpty());
            return true;
        }
        auto* originator = node->GetOriginator();
        if (!originator) {
            break;
        }
        node = originator->As<TCompositeCypressNode>();
    }
    return false;
}

const TCompositeCypressNode::TPersistentAttributes* TCompositeCypressNode::FindAttributes() const
{
    return Attributes_.get();
}

bool TCompositeCypressNode::CompareInheritableAttributes(
    const TTransientAttributes& attributes,
    ENodeMaterializationReason reason) const
{
    if (!HasInheritableAttributes()) {
        return attributes.AreEmpty();
    }

#define XX(camelCaseName, snakeCaseName) \
    if (TryGet##camelCaseName() != attributes.camelCaseName.ToOptional()) { \
        return false; \
    }

    if (reason == ENodeMaterializationReason::Copy) {
        FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(XX)
    } else {
        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
    }
#undef XX

    return true;
}

TConstInheritedAttributeDictionaryPtr TCompositeCypressNode::MaybePatchInheritableAttributes(
    const TConstInheritedAttributeDictionaryPtr& attributes,
    ENodeMaterializationReason reason) const
{
    if (CompareInheritableAttributes(attributes->Attributes())) {
        return attributes;
    }

    auto updatedInheritedAttributeDictionary = New<TInheritedAttributeDictionary>(attributes);
    auto& underlyingAttributes = updatedInheritedAttributeDictionary->MutableAttributes();

#define XX(camelCaseName, snakeCaseName) \
    if (auto inheritedValue = TryGet##camelCaseName()) { \
        underlyingAttributes.camelCaseName.Set(*inheritedValue); \
    }

    if (reason == ENodeMaterializationReason::Copy) {
        FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(XX)
    } else {
        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
    }
#undef XX

    return updatedInheritedAttributeDictionary;
}

void TCompositeCypressNode::FillInheritableAttributes(TTransientAttributes* attributes, ENodeMaterializationReason reason) const
{
#define XX(camelCaseName, snakeCaseName) \
    if (!attributes->camelCaseName.IsSet()) { \
        if (auto inheritedValue = TryGet##camelCaseName()) { \
            attributes->camelCaseName.Set(*inheritedValue); \
        } \
    }

    if (HasInheritableAttributes()) {
        if (reason == ENodeMaterializationReason::Copy) {
            FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(XX)
        } else {
            FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
        }
    }
#undef XX
}

void TCompositeCypressNode::SetAttributes(const TPersistentAttributes* attributes)
{
    if (!attributes || attributes->AreEmpty()) {
        Attributes_.reset();
    } else if (Attributes_) {
        *Attributes_ = attributes->Clone();
    } else {
        Attributes_ = std::make_unique<TPersistentAttributes>(attributes->Clone());
    }
}

void TCompositeCypressNode::CloneAttributesFrom(const TCompositeCypressNode* sourceNode)
{
    auto* attributes = sourceNode->FindAttributes();
    SetAttributes(attributes);
}

void TCompositeCypressNode::MergeAttributesFrom(const TCompositeCypressNode* branchedNode)
{
    auto* attributes = branchedNode->FindAttributes();
    if (!attributes) {
        return;
    }

    if (!Attributes_) {
        SetAttributes(attributes);
        return;
    }

#define XX(camelCaseName, snakeCaseName) \
    Attributes_->camelCaseName.Merge(attributes->camelCaseName, IsTrunk());

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX
}

#define XX(camelCaseName, snakeCaseName) \
const decltype(std::declval<TCompositeCypressNode::TPersistentAttributes>().camelCaseName)* TCompositeCypressNode::DoTryGet##camelCaseName() const \
{ \
    return Attributes_ ? &Attributes_->camelCaseName : nullptr; \
} \
\
auto TCompositeCypressNode::TryGet##camelCaseName() const -> std::optional<TRawVersionedBuiltinAttributeType<T##camelCaseName>> \
{ \
    using TAttribute = decltype(TPersistentAttributes::camelCaseName); \
    return TAttribute::TryGet(&TCompositeCypressNode::DoTryGet##camelCaseName, this); \
} \
\
bool TCompositeCypressNode::Has##camelCaseName() const \
{ \
    return TryGet##camelCaseName().has_value(); \
} \
\
void TCompositeCypressNode::Set##camelCaseName(TCompositeCypressNode::T##camelCaseName value) \
{ \
    if (!Attributes_) { \
        Attributes_ = std::make_unique<TPersistentAttributes>(); \
    } \
    Attributes_->camelCaseName.Set(std::move(value)); \
} \
\
void TCompositeCypressNode::Remove##camelCaseName() \
{ \
    if (!Attributes_) { \
        return; \
    } \
\
    if (IsTrunk()) { \
        Attributes_->camelCaseName.Reset(); \
    } else { \
        Attributes_->camelCaseName.Remove(); \
    } \
\
    if (Attributes_->AreEmpty()) { \
        Attributes_.reset(); \
    } \
}

FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX

////////////////////////////////////////////////////////////////////////////////

void GatherInheritableAttributes(
    const TCypressNode* node,
    TCompositeCypressNode::TTransientAttributes* attributes,
    ENodeMaterializationReason reason)
{
    for (auto* ancestor = node; ancestor && !attributes->AreFull(); ancestor = ancestor->GetParent()) {
        ancestor->As<TCompositeCypressNode>()->FillInheritableAttributes(attributes, reason);
    }
}

////////////////////////////////////////////////////////////////////////////////

TInheritedAttributeDictionary::TInheritedAttributeDictionary(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

TInheritedAttributeDictionary::TInheritedAttributeDictionary(
    const TBootstrap* bootstrap,
    NYTree::IAttributeDictionaryPtr&& attributes)
    : Bootstrap_(bootstrap)
{
    for (const auto& [key, value] : attributes->ListPairs()){
        SetYson(key, value);
    }
}

TInheritedAttributeDictionary::TInheritedAttributeDictionary(const TConstInheritedAttributeDictionaryPtr& other)
    : TInheritedAttributeDictionary(
        other->Bootstrap_,
        other->Clone())
{ }

auto TInheritedAttributeDictionary::ListKeys() const -> std::vector<TKey>
{
    std::vector<TKey> result;
#define XX(camelCaseName, snakeCaseName) \
    if (InheritedAttributes_.camelCaseName.IsSet()) {  \
        result.push_back(#snakeCaseName); \
    }

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (Fallback_) {
        auto fallbackKeys = Fallback_->ListKeys();
        result.insert(result.end(), fallbackKeys.begin(), fallbackKeys.end());
        SortUnique(result);
    }

    return result;
}

auto TInheritedAttributeDictionary::ListPairs() const -> std::vector<TKeyValuePair>
{
    return ListAttributesPairs(*this);
}

auto TInheritedAttributeDictionary::FindYson(TKeyView key) const -> TValue
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        auto optionalValue = InheritedAttributes_.camelCaseName.ToOptional(); \
        return optionalValue ? ConvertToYsonString(*optionalValue) : TYsonString(); \
    } \

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);
#undef XX

    if (key == EInternedAttributeKey::PrimaryMedium.Unintern()) {
        auto optionalPrimaryMediumIndex = InheritedAttributes_.PrimaryMediumIndex.ToOptional();
        if (!optionalPrimaryMediumIndex) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(*optionalPrimaryMediumIndex);
        return ConvertToYsonString(medium->GetName());
    }

    if (key == EInternedAttributeKey::HunkPrimaryMedium.Unintern()) {
        auto optionalHunkPrimaryMediumIndex = InheritedAttributes_.HunkPrimaryMediumIndex.ToOptional();
        if (!optionalHunkPrimaryMediumIndex) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(*optionalHunkPrimaryMediumIndex);
        return ConvertToYsonString(medium->GetName());
    }

    if (key == EInternedAttributeKey::Media.Unintern()) {
        auto optionalReplication = InheritedAttributes_.Media.ToOptional();
        if (!optionalReplication) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return ConvertToYsonString(TSerializableChunkReplication(*optionalReplication, chunkManager));
    }

    if (key == EInternedAttributeKey::HunkMedia.Unintern()) {
        auto optionalReplication = InheritedAttributes_.HunkMedia.ToOptional();
        if (!optionalReplication) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return ConvertToYsonString(TSerializableChunkReplication(*optionalReplication, chunkManager));
    }

    if (key == EInternedAttributeKey::TabletCellBundle.Unintern()) {
        auto optionalCellBundle = InheritedAttributes_.TabletCellBundle.ToOptional();
        if (!optionalCellBundle) {
            return {};
        }
        YT_VERIFY(*optionalCellBundle);
        return ConvertToYsonString((*optionalCellBundle)->GetName());
    }

    if (key == EInternedAttributeKey::ChaosCellBundle.Unintern()) {
        auto optionalCellBundle = InheritedAttributes_.ChaosCellBundle.ToOptional();
        if (!optionalCellBundle) {
            return {};
        }
        YT_VERIFY(*optionalCellBundle);
        return ConvertToYsonString((*optionalCellBundle)->GetName());
    }

    return Fallback_ ? Fallback_->FindYson(key) : TYsonString();
}

void TInheritedAttributeDictionary::SetYson(TKeyView key, const TYsonString& value)
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        if (key == EInternedAttributeKey::CompressionCodec.Unintern()) { \
            const auto& chunkManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager; \
            ValidateCompressionCodec( \
                value, \
                chunkManagerConfig->ForbiddenCompressionCodecs, \
                chunkManagerConfig->ForbiddenCompressionCodecNameToAlias); \
        } \
        if (key == EInternedAttributeKey::ErasureCodec.Unintern()) { \
            ValidateErasureCodec( \
                value, \
                Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ForbiddenErasureCodecs); \
        } \
        using TAttr = decltype(InheritedAttributes_.camelCaseName)::TValue; \
        InheritedAttributes_.camelCaseName.Set(ConvertTo<TAttr>(value)); \
        return; \
    }

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (key == EInternedAttributeKey::PrimaryMedium.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mediumName = ConvertTo<std::string>(value);
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        InheritedAttributes_.PrimaryMediumIndex.Set(medium->GetIndex());
        return;
    }

    if (key == EInternedAttributeKey::HunkPrimaryMedium.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mediumName = ConvertTo<std::string>(value);
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        InheritedAttributes_.HunkPrimaryMediumIndex.Set(medium->GetIndex());
        return;
    }

    if (key == EInternedAttributeKey::Media.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        TChunkReplication replication;
        replication.SetVital(true);
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        InheritedAttributes_.Media.Set(replication);
        return;
    }

    if (key == EInternedAttributeKey::HunkMedia.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        TChunkReplication replication;
        replication.SetVital(true);
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        InheritedAttributes_.HunkMedia.Set(replication);
        return;
    }

    if (key == EInternedAttributeKey::TabletCellBundle.Unintern()) {
        auto bundleName = ConvertTo<std::string>(value);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* bundle = tabletManager->GetTabletCellBundleByNameOrThrow(bundleName, true /*activeLifeStageOnly*/);
        InheritedAttributes_.TabletCellBundle.Set(bundle);
        return;
    }

    if (key == EInternedAttributeKey::ChaosCellBundle.Unintern()) {
        auto bundleName = ConvertTo<std::string>(value);
        const auto& chaosManager = Bootstrap_->GetChaosManager();
        auto* bundle = chaosManager->GetChaosCellBundleByNameOrThrow(bundleName, true /*activeLifeStageOnly*/);
        InheritedAttributes_.ChaosCellBundle.Set(bundle);
        return;
    }

    if (!Fallback_) {
        Fallback_ = CreateEphemeralAttributes();
    }

    Fallback_->SetYson(key, value);
}

bool TInheritedAttributeDictionary::Remove(TKeyView key)
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        InheritedAttributes_.camelCaseName.Reset(); \
        return true; \
    }

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (Fallback_) {
        return Fallback_->Remove(key);
    }

    return false;
}

TCompositeCypressNode::TTransientAttributes& TInheritedAttributeDictionary::MutableAttributes()
{
    return InheritedAttributes_;
}

const TCompositeCypressNode::TTransientAttributes& TInheritedAttributeDictionary::Attributes() const
{
    return InheritedAttributes_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
