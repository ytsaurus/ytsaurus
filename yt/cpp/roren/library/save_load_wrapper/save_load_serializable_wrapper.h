#pragma once

///
/// @file save_load_serializable_wrapper.h
///
/// TSaveLoadSerializableWrapper is a wrapper that wraps any T with saveload-able ctor params and makes T also
/// saveload-able.
/// It can only be used when (de-)serializing classes within the same binary (but not necessarily the same process).

#include <yt/cpp/roren/library/save_load_wrapper/save_load_wrapper.h>

#include <util/stream/str.h>
#include <util/ysaveload.h>

#include <optional>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class ISaveLoadSerializableWrapperImpl
{
public:
    virtual const T& Get() const = 0;
    virtual T& Get() = 0;
    virtual void Save(IOutputStream*) const = 0;
    virtual void Load(IInputStream*) = 0;
    virtual ~ISaveLoadSerializableWrapperImpl() = default;
};

/// @brief Wraps T with SaveLoad-able ctor args with a SaveLoad implementation for T.
///        Should only be used for inheritance, if you want a plain wrapper - use TSaveLoadSerializableWrapper (below).
template <typename T, typename ...TCtorArgs>
class TSaveLoadSerializableWrapperImpl : public ISaveLoadSerializableWrapperImpl<T>
{
public:

    virtual const T& Get() const override
    {
        return *Value_;
    }

    virtual T& Get() override
    {
        return *Value_;
    }

    TSaveLoadSerializableWrapperImpl() = default;

    TSaveLoadSerializableWrapperImpl(TCtorArgs... args)
    {
        Value_.emplace(args...);
        SerializedArgs_ = Serialize(args...);
    }

    virtual void Save(IOutputStream* s) const override
    {
        s->Write(SerializedArgs_);
    }

    virtual void Load(IInputStream* s) override
    {
        std::tuple args{Deserialize<TCtorArgs>(s)...};
        auto constrctValue = [&](const TCtorArgs&... args) { Value_.emplace(args...); };
        std::apply(constrctValue, args);
        SerializedArgs_ = std::apply(Serialize, args);
    }

private:
    template<typename TDeserialized>
    static TDeserialized Deserialize(IInputStream* s)
    {
        TDeserialized value;
        ::Load(s, value);
        return value;
    }

    static std::string Serialize(const TCtorArgs&... args)
    {
        TStringStream ss;
        ::SaveMany(&ss, args...);
        return ss.Str();
    }

private:
    std::optional<T> Value_;
    std::string SerializedArgs_;
};

/// @brief Wraps T with SaveLoad-able ctor args with a SaveLoad implementation for T.
template <typename T>
using TSaveLoadSerializableWrapper = TSaveLoadWrapper<ISaveLoadSerializableWrapperImpl<T>>;

// This is sometimes needed in cases with multiple inheritance to properly overwrite Save and Load ptrs in vtable.
#define USE_SAVELOAD_SERIALIZABLE_WRAPPER_OVERRIDES(TSaveLoadBase) \
    virtual void Save(IOutputStream* s) const override \
    { \
        TSaveLoadBase::Save(s); \
    } \
    virtual void Load(IInputStream* s) override \
    { \
        TSaveLoadBase::Load(s); \
    }

template <typename T, typename ...TCtorArgs>
TSaveLoadSerializableWrapper<T> MakeSaveLoadSerializable(TCtorArgs... args)
{
    return TSaveLoadSerializableWrapper<T>(std::make_shared<TSaveLoadSerializableWrapperImpl<T, TCtorArgs...>>(args...));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
