#include "state.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

IYsonSerializable& AsSerializable(const IStateHolderPtr& state)
{
    auto* serializable = dynamic_cast<IYsonSerializable*>(state.Get());
    YT_VERIFY(serializable);
    return *serializable;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TStateManagerMock::Contains(const std::string& name) const
{
    return Storage_.contains(name);
}

NYson::TYsonString TStateManagerMock::Get(const std::string& name) const
{
    return Storage_.at(name);
}

std::optional<NYson::TYsonString> TStateManagerMock::TryGet(const std::string& name) const
{
    if (auto iter = Storage_.find(name); iter != Storage_.end()) {
        return iter->second;
    }
    return std::nullopt;
}

void TStateManagerMock::Set(const std::string& name, const NYson::TYsonString& value)
{
    Storage_[name] = value;
    States_.erase(name);
}

void TStateManagerMock::Erase(const std::string& name)
{
    Storage_.erase(name);
    States_.erase(name);
}

THashMap<std::string, NYson::TYsonString> TStateManagerMock::GetStorage() const
{
    return Storage_;
}

void TStateManagerMock::SetStorage(THashMap<std::string, NYson::TYsonString>&& storage)
{
    Storage_ = std::move(storage);
}

IInitContextPtr TStateManagerMock::CreateContext(std::string prefix)
{
    if (!prefix.empty()) {
        prefix = ExtendStateNamePrefix({}, prefix);
    }
    return New<TInitContextMock>(MakeStrong(this), std::move(prefix));
}

void TStateManagerMock::Sync()
{
    for (auto iter = States_.begin(); iter != States_.end();) {
        if (auto strongState = iter->second.Lock()) {
            if (auto serialized = AsSerializable(strongState).Serialize()) {
                Storage_[iter->first] = *serialized;
            } else {
                Storage_.erase(iter->first);
            }
            ++iter;
        } else {
            States_.erase(iter++);
        }
    }
}

IStateHolderPtr TStateManagerMock::CreateState(const std::string& name, std::function<IStateHolderPtr()> ctor)
{
    YT_VERIFY(!States_.contains(name) || States_[name].IsExpired());

    auto state = ctor();
    if (const auto serialized = TryGet(name)) {
        AsSerializable(state).Deserialize(*serialized);
    }
    States_[name] = state;

    return state;
}

////////////////////////////////////////////////////////////////////////////////

TMutableStateProviderMock::TMutableStateProviderMock(
    TStateManagerMockPtr storage,
    std::string name,
    std::function<IStateHolderPtr()> ctor)
    : State_(storage->CreateState(name, ctor))
{ }

IStateHolderPtr TMutableStateProviderMock::GetState()
{
    return State_;
}

////////////////////////////////////////////////////////////////////////////////

TInitContextMock::TInitContextMock(
    TStateManagerMockPtr storage,
    std::string prefix)
    : Storage_(std::move(storage))
    , Prefix_(std::move(prefix))
{ }

IInitContextPtr TInitContextMock::WithPrefix(TStringBuf prefix) const
{
    return New<TInitContextMock>(Storage_, ExtendStateNamePrefix(Prefix_, prefix));
}

const std::string& TInitContextMock::GetPrefix() const
{
    return Prefix_;
}

TFuture<IMutableStateProviderPtr> TInitContextMock::CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const
{
    return MakeFuture<IMutableStateProviderPtr>(New<TMutableStateProviderMock>(Storage_, GetPrefix(), std::move(ctor)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
