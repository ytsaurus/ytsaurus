#ifndef RUNTIME_INIT_CONTEXT_INL_H_
    #error "Direct inclusion of this file is not allowed, include runtime_init_context.h"
    // For the sake of sane code completion.
    #include "runtime_init_context.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TStateHolder>
TFuture<TMutableStateKeyClient<TStateHolder>> IRuntimeInitContext::CreateMutableStateKeyClient(TStringBuf name) const
{
    return WithPrefix(name)->CreateMutableStateKeyClient<TStateHolder>();
}

template <class TStateHolder>
TFuture<TMutableStateKeyClient<TStateHolder>> IRuntimeInitContext::CreateMutableStateKeyClient() const
{
    return CreateMutableStateKeyProvider(&New<TYsonSerializableStateHolder<TStateHolder>>)
        .AsUnique()
        .Apply(BIND([] (TErrorOr<IMutableStateKeyProviderPtr>&& result) {
            return TMutableStateKeyClient<TStateHolder>(std::move(result.ValueOrThrow()));
        }));
}

template <class TStateHolder>
void IRuntimeInitContext::InitClient(TMutableStateKeyClient<TStateHolder>& client) const
{
    client = NConcurrency::WaitFor(CreateMutableStateKeyClient<TStateHolder>()).ValueOrThrow();
}

template <class TStateHolder>
void IRuntimeInitContext::InitClient(TMutableStateKeyClient<TStateHolder>& client, TStringBuf name) const
{
    WithPrefix(name)->InitClient<TStateHolder>(client);
}

template <class TStateHolder>
TFuture<TJoinedStateKeyClient<TStateHolder>> IRuntimeInitContext::CreateJoinedStateKeyClient() const
{
    return CreateJoinedStateKeyProvider(&New<TYsonSerializableStateHolder<TStateHolder>>)
        .AsUnique()
        .Apply(BIND([] (TErrorOr<IJoinedStateKeyProviderPtr>&& result) {
            return TJoinedStateKeyClient<TStateHolder>(std::move(result.ValueOrThrow()));
        }));
}

template <class TStateHolder>
void IRuntimeInitContext::InitClient(TJoinedStateKeyClient<TStateHolder>& client) const
{
    client = NConcurrency::WaitFor(CreateJoinedStateKeyClient<TStateHolder>()).ValueOrThrow();
}

template <class TStateHolder>
void IRuntimeInitContext::InitClient(TJoinedStateKeyClient<TStateHolder>& client, TStringBuf name) const
{
    WithPrefix(name)->InitClient<TStateHolder>(client);
}

template <class TStateHolder>
void IRuntimeInitContext::InitExternalStateClient(TMutableStateKeyClient<TStateHolder>& client) const
{
    auto manager = GetExternalStateManagerOrThrow(GetPrefix());
    manager->ValidateStateClass(typeid(TStateHolder));
    client = TMutableStateKeyClient<TStateHolder>(std::move(manager));
}

template <class TStateHolder>
void IRuntimeInitContext::InitExternalStateClient(TJoinedStateKeyClient<TStateHolder>& client) const
{
    auto joiner = GetExternalStateJoinerOrThrow(GetPrefix());
    joiner->ValidateStateClass(typeid(TStateHolder));
    client = TJoinedStateKeyClient<TStateHolder>(std::move(joiner));
}

template <class TStateHolder>
void IRuntimeInitContext::InitExternalStateClient(TMutableStateKeyClient<TStateHolder>& client, TStringBuf name) const
{
    WithPrefix(name)->InitExternalStateClient<TStateHolder>(client);
}

template <class TStateHolder>
void IRuntimeInitContext::InitExternalStateClient(TJoinedStateKeyClient<TStateHolder>& client, TStringBuf name) const
{
    WithPrefix(name)->InitExternalStateClient<TStateHolder>(client);
}

template <class T>
TIntrusivePtr<T> IRuntimeInitContext::GetParameters() const
{
    auto object = GetParametersObject();
    if (!object) {
        // The spec names no processing function (possible only under the test environment):
        // the static block is absent and defaults apply.
        return New<T>();
    }
    auto parameters = DynamicPointerCast<T>(object);
    THROW_ERROR_EXCEPTION_UNLESS(parameters,
        "Static function parameters type mismatch: requested %Qv, registered %Qv",
        TypeName<T>(),
        TypeName(*object));
    return parameters;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
