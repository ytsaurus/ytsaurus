#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TStateManagerMock
    : public TRefCounted
{
public:
    TStateManagerMock() = default;

    bool Contains(const std::string& name) const;
    NYson::TYsonString Get(const std::string& name) const;

    std::optional<NYson::TYsonString> TryGet(const std::string& name) const;

    void Set(const std::string& name, const NYson::TYsonString& value);
    void Erase(const std::string& name);

    THashMap<std::string, NYson::TYsonString> GetStorage() const;
    void SetStorage(THashMap<std::string, NYson::TYsonString>&& storage);

    IInitContextPtr CreateContext(std::string prefix = "");

    void Sync();

    IStateHolderPtr CreateState(const std::string& name, std::function<IStateHolderPtr()> ctor);

private:
    THashMap<std::string, NYson::TYsonString> Storage_;
    THashMap<std::string, TWeakPtr<IStateHolder>> States_;
};

DEFINE_REFCOUNTED_TYPE(TStateManagerMock);

////////////////////////////////////////////////////////////////////////////////

class TMutableStateProviderMock
    : public IMutableStateProvider
{
public:
    TMutableStateProviderMock(
        TStateManagerMockPtr storage,
        std::string name,
        std::function<IStateHolderPtr()> ctor);
    IStateHolderPtr GetState() override;

private:
    const IStateHolderPtr State_;
};

DEFINE_REFCOUNTED_TYPE(TMutableStateProviderMock);

////////////////////////////////////////////////////////////////////////////////

class TInitContextMock
    : public IInitContext
{
public:
    explicit TInitContextMock(
        TStateManagerMockPtr storage,
        std::string prefix);

    IInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

    TFuture<IMutableStateProviderPtr> CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const override;

private:
    const TStateManagerMockPtr Storage_;
    const std::string Prefix_;
};

DEFINE_REFCOUNTED_TYPE(TInitContextMock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
