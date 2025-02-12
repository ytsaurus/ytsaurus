#pragma once

#include "fwd.h"

#include "serializable.h"
#include "../key_value.h"
#include "../noncodable.h"

#include <util/generic/ptr.h>
#include <util/stream/mem.h>
#include <util/stream/zerocopy_output.h>
#include <util/system/type_name.h>

#include <library/cpp/yt/assert/assert.h>

#include <utility>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename>
struct TIsRowCoderRequired : public std::true_type
{ };

template <typename T>
constexpr bool IsRowCoderRequired = TIsRowCoderRequired<T, void>::value;

template <typename T>
IRawCoderPtr MakeDefaultRawCoder();

////////////////////////////////////////////////////////////////////////////////

class IRawCoder
    : public ISerializable<IRawCoder>
{
public:
    virtual void EncodeRow(IZeroCopyOutput* output, const void* row) = 0;
    virtual void DecodeRow(TStringBuf input, void* row) = 0;
    [[nodiscard]] virtual std::pair<IRawCoderPtr, IRawCoderPtr> UnpackKV() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TDefaultRawCoder
    : public IRawCoder
{
public:
    void EncodeRow(IZeroCopyOutput* output, const void* row) override
    {
        Coder_.Encode(output, *static_cast<const T*>(row));
    }

    void DecodeRow(TStringBuf input, void* row) override
    {
        TMemoryInput stream(input);
        Coder_.Decode(&stream, *static_cast<T*>(row));
    }

    [[nodiscard]] std::pair<IRawCoderPtr, IRawCoderPtr> UnpackKV() const override
    {
        if constexpr (NTraits::IsTKV<T>) {
            return {
                MakeDefaultRawCoder<typename T::TKey>(),
                MakeDefaultRawCoder<typename T::TValue>(),
            };
        } else {
            ythrow yexception() << TypeName<T>() << " is not TKV<?, ?>";
        }
    }

private:
    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawCoderPtr {
            return ::MakeIntrusive<TDefaultRawCoder<T>>();
        };
    }

    void Save(IOutputStream* /*stream*/) const override
    { }

    void Load(IInputStream* /*stream*/) override
    { }

private:
    TCoder<T> Coder_ = {};
};

template <typename T>
IRawCoderPtr MakeDefaultRawCoder()
{
    if constexpr (CImplicitlyCodable<T>) {
        return ::MakeIntrusive<TDefaultRawCoder<T>>();
    } else if constexpr (IsRowCoderRequired<T>) {
        static_assert(TDependentFalse<T>, "You must provide TCoder implementation (via saveload or Roren*code)");
    } else {
        YT_UNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
