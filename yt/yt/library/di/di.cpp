#include "di.h"

#include <library/cpp/yt/memory/new.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/misc/finally.h>

#include <util/generic/hash.h>
#include <util/generic/scope.h>

namespace NYT::NDI {

////////////////////////////////////////////////////////////////////////////////

struct TComponentImpl
    : public TRefCounted
{
    THashSet<TComponent::TProvider> Providers;
    THashSet<TComponent::TTypeCast> TypeCasts;
};

DEFINE_REFCOUNTED_TYPE(TComponentImpl)

////////////////////////////////////////////////////////////////////////////////

TComponent::TProvider::operator size_t() const
{
    size_t hash = THash<TString>()(Name);
    hash = CombineHashes(hash, static_cast<size_t>(Provided));
    for (const auto& id : Required) {
        hash = CombineHashes(hash, static_cast<size_t>(id));
    }
    for (const auto& id : CycleRequired) {
        hash = CombineHashes(hash, static_cast<size_t>(id));
    }
    return hash;
}

bool TComponent::TProvider::HasSameId(const TProvider& other) const
{
    if (IsConstructor && other.IsConstructor) {
        return true;
    }

    if (ProviderId && other.ProviderId) {
        return ProviderId == other.ProviderId;
    }

    return false;
}

bool TComponent::TProvider::operator == (const TProvider& other) const
{
    auto fields = [] (const auto& other) {
        return std::tie(
            other.Name,
            other.Provided,
            other.Required,
            other.CycleRequired);
    };

    return fields(*this) == fields(other) && HasSameId(other);
}

TComponent::TTypeCast::operator size_t() const
{
    return CombineHashes(
        static_cast<size_t>(From),
        static_cast<size_t>(To));
}

bool TComponent::TTypeCast::operator == (const TTypeCast& other) const
{
    return std::tie(From, To) == std::tie(other.From, other.To);
}

////////////////////////////////////////////////////////////////////////////////

TComponent::TComponent()
    : Impl_(New<TComponentImpl>())
{ }

void TComponent::AddProvider(const TProvider& provider)
{
    Impl_->Providers.insert(provider);
}

void TComponent::AddTypeCast(const TTypeCast& typecast)
{
    Impl_->TypeCasts.insert(typecast);
}

TComponent TComponent::Install(const TComponent& other)
{
    for (const auto& otherProvider : other.Impl_->Providers) {
        Impl_->Providers.insert(otherProvider);
    }

    for (const auto& typecast : other.Impl_->TypeCasts) {
        Impl_->TypeCasts.insert(typecast);
    }

    return *this;
}

std::optional<TComponent::TProvider> TComponent::FindProvider(const TObjectId& objectId)
{
    THashSet<TProvider> matched;
    for (const auto& provider : Impl_->Providers) {
        if (provider.Provided == objectId) {
            matched.insert(provider);
        }
    }

    for (const auto& cast : Impl_->TypeCasts) {
        if (cast.To == objectId.GetTypeId()) {
            matched.insert(TProvider{
                .Provided = objectId,
                .Required = {objectId.CastTo(cast.From)},
                .Constructor = [cast, objectId] (TInjector* injector) {
                    cast.Cast(injector, objectId.CastTo(cast.From), objectId);
                }
            });
        }
    }

    if (matched.empty()) {
        return {};
    } else if (matched.size() > 1) {
        THROW_ERROR_EXCEPTION("Found multiple providers for object")
            << TErrorAttribute("object_id", ToString(objectId));
    }

    return *matched.begin();
}

////////////////////////////////////////////////////////////////////////////////

struct TObject
{
    std::any Storage;

    std::optional<TObjectId> CreatingDependency;
    bool CreatingSelf = false;

    std::vector<std::function<void(std::any)>> Subscribers;
};

struct TInjectorImpl
    : public TRefCounted
{
    TComponent Component;

    std::optional<TInjector> Parent;

    TObjectId SelfId = TObjectId::Get<TInjector>();

    THashMap<TObjectId, TObject> Objects;
};

DEFINE_REFCOUNTED_TYPE(TInjectorImpl)

////////////////////////////////////////////////////////////////////////////////

TInjector::TInjector(const TComponent& component)
    : Impl_(New<TInjectorImpl>())
{
    Impl_->Component = component;
}

TInjector::~TInjector()
{ }

std::vector<TString> TInjector::DumpCycle(TObjectId objectId)
{
    auto currentObjectId = objectId;

    std::vector<TString> cycle;
    do {
        cycle.push_back(ToString(currentObjectId));

        const auto& object = Impl_->Objects[currentObjectId];

        YT_VERIFY(object.CreatingDependency);
        currentObjectId = *object.CreatingDependency;
    } while (currentObjectId != objectId);

    return cycle;
}

std::any TInjector::DoGet(TObjectId objectId)
{
    if (objectId == Impl_->SelfId) {
        return *this;
    }

    auto it = Impl_->Objects.find(objectId);
    if (it != Impl_->Objects.end() && it->second.Storage.has_value()) {
        return it->second.Storage;
    }

    if (it != Impl_->Objects.end() && it->second.CreatingDependency) {
        THROW_ERROR_EXCEPTION("Dependency cycle found")
            << TErrorAttribute("cycle", DumpCycle(objectId));
    }

    if (it != Impl_->Objects.end() && it->second.CreatingSelf) {
        THROW_ERROR_EXCEPTION("Dependency cycle found")
            << TErrorAttribute("object_id", ToString(objectId));
    }

    if (CreateObject(objectId)) {
        const auto& object = Impl_->Objects[objectId];
        YT_VERIFY(object.Storage.has_value());
        return object.Storage;
    } else if (Impl_->Parent) {
        return Impl_->Parent->DoGet(objectId);
    } else {
        THROW_ERROR_EXCEPTION("Missing provider for object")
            << TErrorAttribute("object_id", ToString(objectId));
    }
}

bool TInjector::CreateObject(TObjectId objectId)
{
    auto provider = Impl_->Component.FindProvider(objectId);
    if (!provider) {
        return false;
    }

    auto& object = Impl_->Objects[objectId];
    for (const auto& dependencyId : provider->Required) {
        object.CreatingDependency = dependencyId;
        auto finally = Finally([&] {
            object.CreatingDependency = {};
        });

        DoGet(dependencyId);
    }

    {
        object.CreatingSelf = true;
        auto finally = Finally([&] {
            object.CreatingSelf = false;
        });
        provider->Constructor(this);
    }

    for (const auto& dependencyId : provider->CycleRequired) {
        if (!Impl_->Objects[dependencyId].CreatingDependency) {
            DoGet(dependencyId);
        }
    }

    return true;
}

void TInjector::SubscribeCreate(TObjectId objectId, std::function<void(std::any)> cb)
{
    auto& object = Impl_->Objects[objectId];
    if (object.Storage.has_value()) {
        cb(object.Storage);
    } else {
        object.Subscribers.push_back(cb);
    }
}

void TInjector::DoSet(TObjectId objectId, std::any value)
{
    auto& object = Impl_->Objects[objectId];
    if (object.Storage.has_value()) {
        THROW_ERROR_EXCEPTION("Object already registered")
            << TErrorAttribute("object_id", ToString(objectId));
    }

    object.Storage = value;
    for (const auto& cb : object.Subscribers) {
        cb(value);
    }
    object.Subscribers.clear();
}

TInjector TInjector::NewScope(const TComponent& component)
{
    TInjector injector{component};
    injector.Impl_->Parent = *this;
    return injector;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDI
