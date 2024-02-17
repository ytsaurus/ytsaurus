#pragma once

#include "fwd.h"
#include "raw_coder.h"

#include "../coder.h"

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/stream/fwd.h>
#include <util/system/defaults.h>

#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

// Returns pointer to function, that crashes when invoked.
IRawCoderPtr CrashingCoderFactory();
TRowVtable CrashingGetVtableFactory();

////////////////////////////////////////////////////////////////////////////////

struct TRowVtable
{
public:
    using TUniquePtr = std::unique_ptr<void, std::function<void(void*)>>;
    using TUniDataFunction = void (*)(void*);
    using TCopyDataFunction = void (*)(void*, const void*);
    using TRawCoderFactoryFunction = IRawCoderPtr (*)();
    using TRowVtableFactoryFunction = TRowVtable (*)();
    using TCopyToUniquePtrFunction = TUniquePtr (*)(const void*);

public:
    static constexpr ssize_t NotKv = -1;

public:
    TString TypeName = {};
    size_t TypeHash = 0xDEADBEEF;
    ssize_t DataSize = 0;
    TUniDataFunction DefaultConstructor = nullptr;
    TUniDataFunction Destructor = nullptr;
    TCopyDataFunction CopyConstructor = nullptr;
    TCopyToUniquePtrFunction CopyToUniquePtr = nullptr;
    TRawCoderFactoryFunction RawCoderFactory = &CrashingCoderFactory;
    ssize_t KeyOffset = NotKv;
    ssize_t ValueOffset = NotKv;
    TRowVtableFactoryFunction KeyVtableFactory = &CrashingGetVtableFactory;
    TRowVtableFactoryFunction ValueVtableFactory = &CrashingGetVtableFactory;

public:
    TRowVtable() = default;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TRowVtable MakeRowVtable()
{
    TRowVtable vtable;

    vtable.TypeName = typeid(T).name();
    vtable.TypeHash = typeid(T).hash_code();

    if constexpr (std::is_same_v<T, void>) {
        auto noop = [] (void* ) {
        };
        auto noopCopy = [] (void* , const void*) {
        };
        vtable.DataSize = 0;
        vtable.Destructor = noop;
        vtable.DefaultConstructor = noop;
        vtable.CopyConstructor = noopCopy;
        vtable.RawCoderFactory = nullptr;
    } else {
        vtable.DataSize = sizeof(T);
        vtable.Destructor = [] (void* data) {
            T* d = reinterpret_cast<T*>(data);
            d->~T();
        };
        vtable.DefaultConstructor = [] (void* data) {
            new(data) T;
        };
        vtable.CopyConstructor = [] (void* destination, const void* source) {
            new(destination) T(*reinterpret_cast<const T*>(source));
        };
        vtable.RawCoderFactory = &MakeDefaultRawCoder<T>;
        vtable.CopyToUniquePtr = [] (const void* p) {
            return TRowVtable::TUniquePtr(
                new T(*reinterpret_cast<const T*>(p)),
                [] (void* p) { delete static_cast<T*>(p); }
            );
        };

        if constexpr (NTraits::IsTKV<T>) {
            vtable.KeyOffset = T::KeyOffset;
            vtable.ValueOffset = T::ValueOffset;
            vtable.KeyVtableFactory = &MakeRowVtable<NTraits::TKeyOfT<T>>;
            vtable.ValueVtableFactory = &MakeRowVtable<NTraits::TValueOfT<T>>;
        }
    }
    return vtable;
}

NYT::TNode SaveToNode(const TRowVtable& rowVtable);
NYT::TNode SaveToNode(const std::vector<TRowVtable>& rowVtables);
TRowVtable LoadVtableFromNode(const NYT::TNode& node);
std::vector<TRowVtable> LoadVtablesFromNode(const NYT::TNode& node);

////////////////////////////////////////////////////////////////////////////////

class TRawRowHolder
{
public:
    TRawRowHolder() = default;

    explicit TRawRowHolder(TRowVtable rowVtable)
        : Data_(rowVtable.DataSize)
        , RowVtable_(std::move(rowVtable))
    {
        if (RowVtable_.DefaultConstructor != nullptr) {
            RowVtable_.DefaultConstructor(GetData());
        }
    }

    TRawRowHolder(const TRawRowHolder& that)
        : Data_(that.RowVtable_.DataSize)
        , RowVtable_(that.RowVtable_)
    {
        if (RowVtable_.CopyConstructor != nullptr) {
            RowVtable_.CopyConstructor(GetData(), that.GetData());
        }
    }

    TRawRowHolder(TRawRowHolder&& that) noexcept
    {
        *this = std::move(that);
    }

    ~TRawRowHolder()
    {
        if (RowVtable_.Destructor != nullptr) {
            RowVtable_.Destructor(GetData());
        }
    }

    TRawRowHolder& operator=(TRawRowHolder&& rhs)
    {
        if (this != &rhs) {
            std::swap(Data_, rhs.Data_);
            std::swap(RowVtable_, rhs.RowVtable_);
        }
        return *this;
    }

    void Reset(const TRowVtable& rowVtable)
    {
        *this = TRawRowHolder(rowVtable);
    }

    void* GetData()
    {
        return Data_.data();
    }

    const void* GetData() const
    {
        return Data_.data();
    }

    void CopyFrom(const void* row)
    {
        Y_ASSERT(RowVtable_.CopyConstructor);
        if (RowVtable_.Destructor != nullptr) {
            RowVtable_.Destructor(GetData());
        }
        RowVtable_.CopyConstructor(GetData(), row);
    }

    void* GetKeyOfKV()
    {
        Y_DEBUG_ABORT_UNLESS(RowVtable_.KeyOffset >= 0,
            "Trying to get key of not TKV type: %s", RowVtable_.TypeName.c_str());
        return Data_.data() + RowVtable_.KeyOffset;
    }

    const void* GetKeyOfKV() const
    {
        Y_DEBUG_ABORT_UNLESS(RowVtable_.KeyOffset >= 0,
            "Trying to get key of not TKV type: %s", RowVtable_.TypeName.c_str());
        return Data_.data() + RowVtable_.KeyOffset;
    }

    void* GetValueOfKV()
    {
        Y_DEBUG_ABORT_UNLESS(RowVtable_.ValueOffset >= 0,
            "Trying to get key of not TKV type: %s", RowVtable_.TypeName.c_str());
        return Data_.data() + RowVtable_.ValueOffset;
    }

    const void* GetValueOfKV() const
    {
        Y_DEBUG_ABORT_UNLESS(RowVtable_.ValueOffset >= 0,
            "Trying to get key of not TKV type: %s", RowVtable_.TypeName.c_str());
        return Data_.data() + RowVtable_.ValueOffset;
    }

    const TRowVtable& GetRowVtable() const
    {
        return RowVtable_;
    }

private:
    std::vector<char> Data_;
    TRowVtable RowVtable_;
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void* GetKeyOfKv(const TRowVtable& rowVtable, void* row)
{
    return static_cast<char*>(row) + rowVtable.KeyOffset;
}

Y_FORCE_INLINE const void* GetKeyOfKv(const TRowVtable& rowVtable, const void* row)
{
    return static_cast<const char*>(row) + rowVtable.KeyOffset;
}

Y_FORCE_INLINE void* GetValueOfKv(const TRowVtable& rowVtable, void* row)
{
    return static_cast<char*>(row) + rowVtable.ValueOffset;
}

Y_FORCE_INLINE const void* GetValueOfKv(const TRowVtable& rowVtable, const void* row)
{
    return static_cast<const char*>(row) + rowVtable.ValueOffset;
}

Y_FORCE_INLINE bool IsKv(const TRowVtable& rowVtable)
{
    return rowVtable.KeyOffset != TRowVtable::NotKv && rowVtable.ValueOffset != TRowVtable::NotKv;
}

Y_FORCE_INLINE bool IsVoid(const TRowVtable& rowVtable)
{
    return rowVtable.DataSize == 0;
}

Y_FORCE_INLINE bool IsDefined(const TRowVtable& rowVtable)
{
    return rowVtable.DataSize > 0;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NRoren::NPrivate

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <>
class TCoder<NPrivate::TRawRowHolder>
{
public:
    void Encode(IOutputStream* out, const NPrivate::TRawRowHolder& rowHolder)
    {
        VtableCoder_.Encode(out, rowHolder.GetRowVtable());

        if (IsDefined(rowHolder.GetRowVtable())) {
            InitKeyCoderIfRequired(rowHolder.GetRowVtable());

            Buffer_.clear();
            {
                auto so = TStringOutput{Buffer_};
                RawCoder_->EncodeRow(&so, rowHolder.GetData());
            }
            ::Save(out, Buffer_);
        }
    }

    void Decode(IInputStream* in, NPrivate::TRawRowHolder& rowHolder)
    {
        auto rowVtable = NPrivate::TRowVtable{};
        VtableCoder_.Decode(in, rowVtable);

        rowHolder = NPrivate::TRawRowHolder{std::move(rowVtable)};

        if (IsDefined(rowVtable)) {
            InitKeyCoderIfRequired(rowVtable);
            Buffer_.clear();
            ::Load(in, Buffer_);
            RawCoder_->DecodeRow(Buffer_, rowHolder.GetData());
        }
    }

private:
    void InitKeyCoderIfRequired(const NPrivate::TRowVtable& rowVtable)
    {
        if (RawCoder_ == nullptr && IsDefined(rowVtable)) {
            RawCoder_ = rowVtable.RawCoderFactory();
        }
    }

private:
    TCoder<NPrivate::TRowVtable> VtableCoder_;
    NPrivate::IRawCoderPtr RawCoder_;
    TString Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

////////////////////////////////////////////////////////////////////////////////

template <>
class TSerializer<NRoren::NPrivate::TRowVtable>
{
public:
    using TRowVtable = NRoren::NPrivate::TRowVtable;

public:
    static void Save(IOutputStream* output, const NRoren::NPrivate::TRowVtable& rowVtable);
    static void Load(IInputStream* input, NRoren::NPrivate::TRowVtable& rowVtable);
};

////////////////////////////////////////////////////////////////////////////////
