#pragma once

#include "fwd.h"
#include "raw_coder.h"
#include "hash.h"
#include "save_loadable_pointer_wrapper.h"

#include "../coder.h"
#include "../noncodable.h"

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
    using TMoveDataFunction = void (*)(void*, void*);
    using TRawCoderFactoryFunction = IRawCoderPtr (*)();
    using TRowVtableFactoryFunction = TRowVtable (*)();
    using TCopyToUniquePtrFunction = TUniquePtr (*)(const void*);
    using TMoveToUniquePtrFunction = TUniquePtr (*)(void*);
    using THashFunction = size_t (*)(const void*);
    using TEqualFunction = bool (*)(const void*, const void*);

public:
    static constexpr ssize_t NotKv = -1;

public:
    TString TypeName = {};
    size_t TypeHash = 0xDEADBEEF;
    ssize_t DataSize = 0;
    TUniDataFunction DefaultConstructor = nullptr;
    TUniDataFunction Destructor = nullptr;
    TCopyDataFunction CopyConstructor = nullptr;
    TMoveDataFunction MoveConstructor = nullptr;
    TCopyToUniquePtrFunction CopyToUniquePtr = nullptr;
    TMoveToUniquePtrFunction MoveToUniquePtr = nullptr;
    THashFunction Hash = nullptr;
    TEqualFunction Equal = nullptr;
    TRawCoderFactoryFunction RawCoderFactory = &CrashingCoderFactory;
    bool IsManuallyNoncodable = false;
    ssize_t KeyOffset = NotKv;
    ssize_t ValueOffset = NotKv;
    TRowVtableFactoryFunction KeyVtableFactory = &CrashingGetVtableFactory;
    TRowVtableFactoryFunction ValueVtableFactory = &CrashingGetVtableFactory;

    Y_SAVELOAD_DEFINE(
        TypeName,
        TypeHash,
        DataSize,
        SaveLoadablePointer(DefaultConstructor),
        SaveLoadablePointer(Destructor),
        SaveLoadablePointer(CopyConstructor),
        SaveLoadablePointer(MoveConstructor),
        SaveLoadablePointer(CopyToUniquePtr),
        SaveLoadablePointer(MoveToUniquePtr),
        SaveLoadablePointer(Hash),
        SaveLoadablePointer(Equal),
        SaveLoadablePointer(RawCoderFactory),
        IsManuallyNoncodable,
        KeyOffset,
        ValueOffset,
        SaveLoadablePointer(KeyVtableFactory),
        SaveLoadablePointer(ValueVtableFactory)
    );

public:
    TRowVtable() = default;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, bool isKey = false, bool isValue = false>
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
        auto noopMove = [] (void* , void*) {
        };
        vtable.DataSize = 0;
        vtable.Destructor = noop;
        vtable.DefaultConstructor = noop;
        vtable.CopyConstructor = noopCopy;
        vtable.MoveConstructor = noopMove;
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
        vtable.MoveConstructor = [] (void* destination, void* source) {
            new(destination) T(std::move(*reinterpret_cast<T*>(source)));
        };
        vtable.RawCoderFactory = &MakeDefaultRawCoder<T>;
        vtable.IsManuallyNoncodable = CManuallyNonCodable<T>;
        vtable.CopyToUniquePtr = [] (const void* p) {
            return TRowVtable::TUniquePtr(
                new T(*reinterpret_cast<const T*>(p)),
                [] (void* p) { delete static_cast<T*>(p); }
            );
        };
        vtable.MoveToUniquePtr = [] (void* p) {
            return TRowVtable::TUniquePtr(
                new T(std::move(*reinterpret_cast<T*>(p))),
                [] (void* p) { delete static_cast<T*>(p); }
            );
        };
        if constexpr (isKey && IsHashable<T>) {
            vtable.Hash = [] (const void* p) -> size_t {
                const T* obj = reinterpret_cast<const T*>(p);
                return NRoren::NPrivate::TRorenHash<T>()(*obj);
            };
        }
        if constexpr (isKey && requires (const T& lhs, const T& rhs) { { lhs == rhs } -> std::convertible_to<bool>; }) {
            vtable.Equal = [] (const void* lhs, const void* rhs) -> bool {
                return *reinterpret_cast<const T*>(lhs) == *reinterpret_cast<const T*>(rhs);
            };
        }

        if constexpr (NTraits::IsTKV<T>) {
            vtable.KeyOffset = T::KeyOffset;
            vtable.ValueOffset = T::ValueOffset;
            vtable.KeyVtableFactory = &MakeRowVtable<NTraits::TKeyOfT<T>, true, false>;
            vtable.ValueVtableFactory = &MakeRowVtable<NTraits::TValueOfT<T>, false, true>;
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

    void MoveFrom(void* row)
    {
        Y_ASSERT(RowVtable_.MoveConstructor);
        if (RowVtable_.Destructor != nullptr) {
            RowVtable_.Destructor(GetData());
        }
        RowVtable_.MoveConstructor(GetData(), row);
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

class THashableKey
{
public:
    static THashableKey GetKeyOfKV(const TRawRowHolder& holder)
    {
        const auto& vtable = holder.GetRowVtable();
        Y_ABORT_IF(vtable.KeyOffset == TRowVtable::NotKv);
        Y_ABORT_IF(vtable.KeyVtableFactory == nullptr);
        TRawRowHolder key(vtable.KeyVtableFactory());
        key.CopyFrom(holder.GetKeyOfKV());
        return THashableKey(std::move(key));
    }

    THashableKey(TRawRowHolder key)
        : Key_(std::move(key))
    {
        const auto& keyVtable = Key_.GetRowVtable();
        Y_ABORT_IF(keyVtable.Hash == nullptr);
        Y_ABORT_IF(keyVtable.Equal == nullptr);
        Hash_ = keyVtable.Hash(Key_.GetData());
    }

    size_t GetHash() const noexcept
    {
        return Hash_;
    }

    bool operator==(const THashableKey& other) const
    {
        Y_ABORT_IF(Key_.GetRowVtable().Equal != other.Key_.GetRowVtable().Equal);
        return Key_.GetRowVtable().Equal(Key_.GetData(), other.Key_.GetData());
    }

    const void* Get() const
    {
        return Key_.GetData();
    }

    void CopyFrom(const void* row)
    {
        Key_.CopyFrom(row);
        Hash_ = Key_.GetRowVtable().Hash(Key_.GetData());
    }

private:
    TRawRowHolder Key_;
    size_t Hash_ = 0;
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
struct THash<NRoren::NPrivate::THashableKey>
{
    inline size_t operator()(const NRoren::NPrivate::THashableKey& key) const noexcept
    {
        return key.GetHash();
    }
};
