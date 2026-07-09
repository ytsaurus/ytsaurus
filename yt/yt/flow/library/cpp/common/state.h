#pragma once

#include "public.h"

#include "key.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/flow/lib/serializer/validate.h>

#include <util/stream/mem.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ValidateStateName(const std::string& name);

//! Concatenates |prefix| with |segment| the way #WithPrefix() should work:
//! if |segment| does not start with ``/`` then one ``/`` is prepended, then
//! result is ``prefix + normalized_segment``. The result is validated via
//! #ValidateStateName(). This lets callers pass either ``WithPrefix("state")``
//! or ``WithPrefix("/state")`` uniformly.
std::string ExtendStateNamePrefix(TStringBuf prefix, TStringBuf segment);

////////////////////////////////////////////////////////////////////////////////

//! Placeholder payload for computations that need the state machinery but keep
//! nothing. Always empty and serializes to nothing.
struct TNullState
{
    bool operator==(const TNullState&) const = default;
};

void Serialize(NYson::IYsonConsumer* consumer, const TNullState& state);
void Deserialize(TNullState& state, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

//! Type-erased refcounted handle. State managers hand it out; the typed clients
//! cast it back to #TStateHolder to reach the payload ``T``.
struct IStateHolder
    : public TRefCounted
{
    virtual bool IsEmpty() const = 0;
    virtual void Clear() = 0;
    virtual NYson::TYsonString ToYsonView() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IStateHolder);

////////////////////////////////////////////////////////////////////////////////

//! Optional non-refcounted mixin: a payload that manages its own #Clear() and
//! #IsEmpty() (these always go as a pair). When ``T`` derives it, #TStateHolder
//! forwards to these methods; otherwise it recreates the payload and compares it
//! to a default.
struct ICustomStateOps
{
    virtual ~ICustomStateOps() = default;

    virtual void Clear() = 0;
    virtual bool IsEmpty() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Optional non-refcounted mixin: custom rendering for read-state introspection,
//! used when the default ``ConvertToYsonString(T)`` is not what should be shown.
struct ICustomYsonView
{
    virtual ~ICustomYsonView() = default;

    virtual NYson::TYsonString ToYsonView() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Optional non-refcounted persistence mixin, used by internal mutable states
//! (#TYsonSerializableStateHolder). The framework casts ``IStateHolder*`` to it when it needs
//! round-trip serialization for checkpointing.
struct IYsonSerializable
{
    virtual ~IYsonSerializable() = default;

    virtual void Deserialize(const NYson::TYsonString& serialized) = 0;
    virtual std::optional<NYson::TYsonString> Serialize() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Universal typed #IStateHolder over a payload ``T``. ``T`` is always held behind a
//! pointer (#TIntrusivePtr if it is ref-counted, ``std::shared_ptr`` otherwise).
//! #Clear()/#IsEmpty()/#ToYsonView() dispatch on the optional #ICustomStateOps
//! / #ICustomYsonView mixins; without them the payload is recreated, compared
//! to a default, and rendered via #ConvertToYsonString() respectively.
template <class T>
class TStateHolder
    : public IStateHolder
{
public:
    TStateHolder()
        : Holder_(Allocate())
    { }

    template <class... TArgs>
        requires std::constructible_from<T, TArgs...>
    explicit TStateHolder(TArgs&&... args)
        : Holder_(Allocate(std::forward<TArgs>(args)...))
    { }

    T& Get() noexcept
    {
        return *Holder_;
    }

    const T& Get() const noexcept
    {
        return *Holder_;
    }

    void Clear() override
    {
        if constexpr (std::derived_from<T, ICustomStateOps>) {
            Get().Clear();
        } else {
            Holder_ = Allocate();
        }
    }

    bool IsEmpty() const override
    {
        if constexpr (std::derived_from<T, ICustomStateOps>) {
            return Get().IsEmpty();
        } else {
            return Get() == Default();
        }
    }

    NYson::TYsonString ToYsonView() const override
    {
        if constexpr (std::derived_from<T, ICustomYsonView>) {
            return Get().ToYsonView();
        } else {
            return NYson::ConvertToYsonString(Get());
        }
    }

private:
    using THolder = std::conditional_t<
        std::derived_from<T, TRefCounted>,
        TIntrusivePtr<T>,
        std::shared_ptr<T>>;

    THolder Holder_;

    template <class... TArgs>
    static THolder Allocate(TArgs&&... args)
    {
        if constexpr (std::derived_from<T, TRefCounted>) {
            return New<T>(std::forward<TArgs>(args)...);
        } else {
            return std::make_shared<T>(std::forward<TArgs>(args)...);
        }
    }

    static const T& Default()
    {
        if constexpr (std::derived_from<T, TRefCounted>) {
            static const auto empty = New<T>();
            return *empty;
        } else {
            static const T empty{};
            return empty;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Internal mutable state: #TStateHolder plus round-trip persistence.
template <class T>
class TYsonSerializableStateHolder
    : public TStateHolder<T>
    , public IYsonSerializable
{
public:
    using TStateHolder<T>::TStateHolder;

    void Deserialize(const NYson::TYsonString& serialized) override
    {
        if (serialized) {
            TMemoryInput input(serialized.AsStringBuf());
            NYson::TYsonPullParser parser(&input, serialized.GetType());
            NYson::TYsonPullParserCursor cursor(&parser);
            using NYTree::Deserialize;
            Deserialize(this->Get(), &cursor);
        }
    }

    std::optional<NYson::TYsonString> Serialize() const override
    {
        if (this->IsEmpty()) {
            return std::nullopt;
        }
        return NYson::ConvertToYsonString(this->Get());
    }
};

template <class T>
using TYsonSerializableStateHolderPtr = TIntrusivePtr<TYsonSerializableStateHolder<T>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
