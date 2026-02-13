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

using namespace NCellMaster;
using namespace NChunkServer;
using namespace NObjectClient;
using namespace NServer;
using namespace NYson;
using namespace NYTree;

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
    Persist(context, OptimizeFor);
    Persist(context, ProfilingMode);
    Persist(context, ProfilingTag);
    Persist(context, ChunkMergerMode);
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
#define XX(FieldName, AttributeKey) \
    Persist(context, FieldName);

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeCypressNode::TAttributes<Transient>::AreFull() const
{
#define XX(FieldName, AttributeKey) \
    && FieldName.IsSet()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeCypressNode::TAttributes<Transient>::AreEmpty() const
{
#define XX(FieldName, AttributeKey) \
    && !FieldName.IsNull()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
TCompositeCypressNode::TPersistentAttributes
TCompositeCypressNode::TAttributes<Transient>::ToPersistent() const
    requires Transient
{
    TPersistentAttributes result;
#define XX(FieldName, AttributeKey) \
    if (FieldName.IsSet()) { \
        result.FieldName.Set(TVersionedBuiltinAttributeTraits<decltype(result.FieldName)::TValue>::FromRaw(FieldName.Unbox())); \
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
#define XX(FieldName, AttributeKey) \
    result.FieldName = FieldName.Clone();
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

#define XX(FieldName, AttributeKey) \
    if (TryGet##FieldName() != attributes.FieldName.ToOptional()) { \
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
    if (CompareInheritableAttributes(attributes->Attributes(), reason)) {
        return attributes;
    }

    auto updatedInheritedAttributeDictionary = New<TInheritedAttributeDictionary>(attributes);
    auto& underlyingAttributes = updatedInheritedAttributeDictionary->MutableAttributes();

#define XX(FieldName, AttributeKey) \
    if (auto inheritedValue = TryGet##FieldName()) { \
        underlyingAttributes.FieldName.Set(*inheritedValue); \
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
#define XX(FieldName, AttributeKey) \
    if (!attributes->FieldName.IsSet()) { \
        if (auto inheritedValue = TryGet##FieldName()) { \
            attributes->FieldName.Set(*inheritedValue); \
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

#define XX(FieldName, AttributeKey) \
    Attributes_->FieldName.Merge(attributes->FieldName, IsTrunk());

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX
}

#define XX(FieldName, AttributeKey) \
const decltype(std::declval<TCompositeCypressNode::TPersistentAttributes>().FieldName)* TCompositeCypressNode::DoTryGet##FieldName() const \
{ \
    return Attributes_ ? &Attributes_->FieldName : nullptr; \
} \
\
auto TCompositeCypressNode::TryGet##FieldName() const -> std::optional<TRawVersionedBuiltinAttributeType<T##FieldName>> \
{ \
    using TAttribute = decltype(TPersistentAttributes::FieldName); \
    return TAttribute::TryGet(&TCompositeCypressNode::DoTryGet##FieldName, this); \
} \
\
bool TCompositeCypressNode::Has##FieldName() const \
{ \
    return TryGet##FieldName().has_value(); \
} \
\
void TCompositeCypressNode::Set##FieldName(TCompositeCypressNode::T##FieldName value) \
{ \
    if (!Attributes_) { \
        Attributes_ = std::make_unique<TPersistentAttributes>(); \
    } \
    Attributes_->FieldName.Set(std::move(value)); \
} \
\
void TCompositeCypressNode::Remove##FieldName() \
{ \
    if (!Attributes_) { \
        return; \
    } \
\
    if (IsTrunk()) { \
        Attributes_->FieldName.Reset(); \
    } else { \
        Attributes_->FieldName.Remove(); \
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
    for (const auto& [key, value] : attributes->ListPairs()) {
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
#define XX(FieldName, AttributeKey) \
    if (InheritedAttributes_.FieldName.IsSet()) {  \
        result.push_back(EInternedAttributeKey::AttributeKey.Unintern()); \
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
    auto doGetSimple = [] (const auto* field) {
        auto optionalValue = field->ToOptional();
        return optionalValue ? ConvertToYsonString(*optionalValue) : TYsonString();
    };

    auto doGetMediumNameOrId = [&] (const auto* field) {
        std::optional<int> mediumIndex = field->ToOptional();
        if (!mediumIndex) {
            return TYsonString();
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(*mediumIndex);
        if (Transferable_) {
            return ConvertToYsonString(FromObjectId(medium->GetId()));
        } else {
            return ConvertToYsonString(medium->GetName());
        }
    };

    auto doGetObjectNameOrId = [&] (const auto* field) {
        auto optionalValue = field->ToOptional();
        if (!optionalValue) {
            return TYsonString();
        } else if (Transferable_) {
            return ConvertToYsonString(FromObjectId((*optionalValue)->GetId()));
        } else {
            return ConvertToYsonString((*optionalValue)->GetName());
        }
    };

    auto doGetMedia = [&] (const auto* field) {
        auto optionalValue = field->ToOptional();
        if (!optionalValue) {
            return TYsonString();
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        if (Transferable_) {
            return ConvertToYsonString(TSerializableTransferableChunkReplication(*optionalValue, chunkManager));
        } else {
            return ConvertToYsonString(TSerializableChunkReplication(*optionalValue, chunkManager));
        }
    };

    auto getCompressionCodec = std::bind(doGetSimple, &InheritedAttributes_.CompressionCodec);
    auto getErasureCodec = std::bind(doGetSimple, &InheritedAttributes_.ErasureCodec);
    auto getPrimaryMedium = std::bind(doGetMediumNameOrId, &InheritedAttributes_.PrimaryMediumIndex);
    auto getHunkPrimaryMedium = std::bind(doGetMediumNameOrId, &InheritedAttributes_.HunkPrimaryMediumIndex);
    auto getReplicationFactor = std::bind(doGetSimple, &InheritedAttributes_.ReplicationFactor);
    auto getChunkMergerMode = std::bind(doGetSimple, &InheritedAttributes_.ChunkMergerMode);
    auto getTabletCellBundle = std::bind(doGetObjectNameOrId, &InheritedAttributes_.TabletCellBundle);
    auto getChaosCellBundle = std::bind(doGetObjectNameOrId, &InheritedAttributes_.ChaosCellBundle);
    auto getMedia = std::bind(doGetMedia, &InheritedAttributes_.Media);
    auto getHunkMedia = std::bind(doGetMedia, &InheritedAttributes_.HunkMedia);

    auto internedKey = TInternedAttributeKey::Lookup(key);
    switch (internedKey) {
    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: \
            return doGetSimple(&InheritedAttributes_.FieldName);
        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);
    #undef XX

    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: \
            return get##AttributeKey();
        FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

        case EInternedAttributeKey::InvalidKey:
        default:
            return Fallback_ ? Fallback_->FindYson(key) : TYsonString();
    }
}

void TInheritedAttributeDictionary::SetYson(TKeyView key, const TYsonString& value)
{
    auto doSet = [&] (auto* field) {
        using TAttr = std::decay_t<decltype(*field)>::TValue; \
        field->Set(ConvertTo<TAttr>(value));
    };

    auto setReplicationFactor = std::bind(doSet, &InheritedAttributes_.ReplicationFactor);
    auto setChunkMergerMode = std::bind(doSet, &InheritedAttributes_.ChunkMergerMode);

    auto setCompressionCodec = [&] {
        const auto& chunkManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
        ValidateCompressionCodec(
            value,
            chunkManagerConfig->ForbiddenCompressionCodecs,
            chunkManagerConfig->ForbiddenCompressionCodecNameToAlias);
        doSet(&InheritedAttributes_.CompressionCodec);
    };

    auto setErasureCodec = [&] {
        ValidateErasureCodec(
            value,
            Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ForbiddenErasureCodecs);
        doSet(&InheritedAttributes_.ErasureCodec);
    };

    auto setMedium = [&] (auto* field) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByNameOrThrow(ConvertTo<std::string>(value));
        field->Set(medium->GetIndex());
    };

    auto setPrimaryMedium = std::bind(setMedium, &InheritedAttributes_.PrimaryMediumIndex);
    auto setHunkPrimaryMedium = std::bind(setMedium, &InheritedAttributes_.HunkPrimaryMediumIndex);

    auto setChaosCellBundle = [&] {
        auto name = ConvertTo<std::string>(value);
        const auto& chaosManager = Bootstrap_->GetChaosManager();
        auto* bundle = chaosManager->GetChaosCellBundleByNameOrThrow(name, /*activeLifeStageOnly*/ true);
        InheritedAttributes_.ChaosCellBundle.Set(bundle);
    };

    auto setTabletCellBundle = [&] {
        auto name = ConvertTo<std::string>(value);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* bundle = tabletManager->GetTabletCellBundleByNameOrThrow(name, /*activeLifeStageOnly*/ true);
        InheritedAttributes_.TabletCellBundle.Set(bundle);
    };

    auto doSetMedia = [&] (auto* field) {
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        TChunkReplication replication;
        replication.SetVital(true);
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        field->Set(replication);
    };

    auto setMedia = std::bind(doSetMedia, &InheritedAttributes_.Media);
    auto setHunkMedia = std::bind(doSetMedia, &InheritedAttributes_.HunkMedia);

    auto internedKey = TInternedAttributeKey::Lookup(key);
    switch (internedKey) {
    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: { \
            using TAttr = decltype(InheritedAttributes_.FieldName)::TValue; \
            InheritedAttributes_.FieldName.Set(ConvertTo<TAttr>(value)); \
            return; \
        }
        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: \
            set##AttributeKey(); \
            return;
        FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

        default:
            // NB: don't use |internedKey| here because it may be
            // InvalidInternedAttribute for not interned attributes.
            if (!Fallback_) {
                Fallback_ = CreateEphemeralAttributes();
            }
            Fallback_->SetYson(key, value);
    }
}

bool TInheritedAttributeDictionary::Remove(TKeyView key)
{
    auto internedKey = TInternedAttributeKey::Lookup(key);

    switch (internedKey) {
    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: { \
            InheritedAttributes_.FieldName.Reset(); \
            return true; \
        }
        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

        default:
            if (Fallback_) {
                return Fallback_->Remove(key);
            }

            return false;
    }
}

TCompositeCypressNode::TTransientAttributes& TInheritedAttributeDictionary::MutableAttributes()
{
    return InheritedAttributes_;
}

const TCompositeCypressNode::TTransientAttributes& TInheritedAttributeDictionary::Attributes() const
{
    return InheritedAttributes_;
}


TInheritedAttributeDictionaryPtr TInheritedAttributeDictionary::ToNonTransferableView() const
{
    auto view = New<TInheritedAttributeDictionary>(MakeStrong(this));
    view->Transferable_ = false;
    return view;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
