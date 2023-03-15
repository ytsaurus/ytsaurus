#pragma once

///
/// @file save_load_wrapper.h
///
/// TSaveLoadWrapper is std::shared_ptr implementing SaveLoad interface.
/// It can be used only when (de-)serializing classes among the same binary.

#include <util/ysaveload.h>

#include <concepts>
#include <cstddef>
#include <memory>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NSaveLoadWrapper {

/// @brief Concept for class that implements intefrace TBase.
template <typename TImpl, typename TBase>
concept CImplementation = std::derived_from<TImpl, TBase> && std::default_initializable<TImpl>;

} // namespace NSaveLoadWrapper

/// @brief std::shared_ptr with SaveLoad implementation.
template <typename TAbstractObject>
class TSaveLoadWrapper : public std::shared_ptr<TAbstractObject>
{
public:
    using TMutableAbstractObject = std::remove_const_t<TAbstractObject>;
    using TPtr = std::shared_ptr<TAbstractObject>;

    using TPtr::get;
    using TPtr::operator*;
    using TPtr::operator->;
    using TPtr::operator bool;

    TSaveLoadWrapper(nullptr_t)
    { }

    TSaveLoadWrapper()
        : TSaveLoadWrapper(nullptr)
    { }

    template <NSaveLoadWrapper::CImplementation<TMutableAbstractObject> T>
    TSaveLoadWrapper(std::shared_ptr<T> data)
        : TPtr(std::move(data))
        , Vtable_(MakeVtable<T>())
    { }

    TSaveLoadWrapper(const TSaveLoadWrapper&) = default;
    TSaveLoadWrapper(TSaveLoadWrapper&&) = default;

    template <NSaveLoadWrapper::CImplementation<TMutableAbstractObject> T>
    TSaveLoadWrapper& operator=(std::shared_ptr<T> other)
    {
        TPtr::operator=(std::move(other));
        Vtable_ = MakeVtable<T>();
        return *this;
    }

    TSaveLoadWrapper& operator=(const TSaveLoadWrapper&) = default;
    TSaveLoadWrapper& operator=(TSaveLoadWrapper&&) = default;

    void Save(IOutputStream* output) const
    {
        if (nullptr != *this) {
            Y_VERIFY(Vtable_.SaveF != nullptr);
            Y_VERIFY(Vtable_.LoadF != nullptr);
            ::SaveMany(output, true, Vtable_);
            Vtable_.SaveF(output, get());
        } else {
            ::Save(output, false);
        }
    }

    void Load(IInputStream* input)
    {
        bool has_value = false;
        ::Load(input, has_value);
        if (!has_value) {
            reset();
        } else {
            ::Load(input, Vtable_);
            TPtr::operator=(Vtable_.LoadF(input));
        }
    }

    void swap(TSaveLoadWrapper& other)
    {
        TPtr::swap(other);
        std::swap(Vtable_, other.Vtable_);
    }

    void reset()
    {
        TPtr::reset();
        Vtable_ = {};
    }

    TSaveLoadWrapper<const TAbstractObject> AsConst() const
    {
        auto const_ptr = std::const_pointer_cast<const TAbstractObject>(static_cast<const TPtr&>(*this));
        typename TSaveLoadWrapper<const TAbstractObject>::TVtable vtable{Vtable_.SaveF, Vtable_.LoadF};
        return TSaveLoadWrapper<const TAbstractObject>(const_ptr, vtable);
    }

private:
    struct TVtable
    {
        using TSaveFunction = void (*)(IOutputStream*, const TAbstractObject*);
        using TLoadFunction = std::shared_ptr<TMutableAbstractObject> (*)(IInputStream*);

        TSaveFunction SaveF = nullptr;
        TLoadFunction LoadF = nullptr;

        void Save(IOutputStream* output) const
        {
            ::Save(output, reinterpret_cast<uintptr_t>(SaveF));
            ::Save(output, reinterpret_cast<uintptr_t>(LoadF));
        }

        void Load(IInputStream* input)
        {
            uintptr_t ptr = 0;
            ::Load(input, ptr);
            SaveF = reinterpret_cast<TSaveFunction>(ptr);
            ::Load(input, ptr);
            LoadF = reinterpret_cast<TLoadFunction>(ptr);
        }
    };

    template <NSaveLoadWrapper::CImplementation<TMutableAbstractObject> T>
    TVtable MakeVtable()
    {
        TVtable vtable;
        vtable.SaveF = [](IOutputStream* output, const TAbstractObject* data) {
            const auto* casted = static_cast<const T*>(data);
            ::Save(output, *casted);
        };
        vtable.LoadF = [](IInputStream* input) -> std::shared_ptr<TMutableAbstractObject> {
            auto casted = std::make_shared<T>();
            ::Load(input, *casted);
            return casted;
        };
        return vtable;
    }

    TSaveLoadWrapper(TPtr ptr, const TVtable& vtable)
        : TPtr(std::move(ptr))
        , Vtable_(vtable)
    { }

    TVtable Vtable_;

    friend class TSaveLoadWrapper<TMutableAbstractObject>;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

