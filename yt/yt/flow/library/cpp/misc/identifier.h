#pragma once

#include <util/generic/ptr.h>
#include <util/stream/fwd.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace NYT::NFlow::NDetail {

////////////////////////////////////////////////////////////////////////////////

// Buffer layout (extra bytes after TIdentifierStringData object), shared between Make() and the
// inline accessors in identifier-inl.h:
//   short (length <= ShortLengthMax): [TShortSize length][str bytes]
//   long  (length >  ShortLengthMax): [TShortSize zero marker][TLongSize length][str bytes]
using TShortSize = std::uint8_t;
using TLongSize = std::uint64_t;
inline constexpr std::size_t ShortLengthMax = std::numeric_limits<TShortSize>::max();
inline constexpr std::size_t ShortHeaderSize = sizeof(TShortSize);
inline constexpr std::size_t LongHeaderSize = sizeof(TShortSize) + sizeof(TLongSize);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDetail

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TIdentifierStringData final
    : public ::TAtomicRefCount<TIdentifierStringData>
{
public:
    std::string_view Data() const noexcept;

    size_t ByteSize() const noexcept;

    static ::TIntrusivePtr<TIdentifierStringData> Make(std::string_view str);

    void operator delete(void* ptr) noexcept;

private:
    char* ExtraPtr() noexcept;
    const char* ExtraPtr() const noexcept;
};

////////////////////////////////////////////////////////////////////////////////

// A strongly-typed identifier string. TTag is used to distinguish different identifier types.
// Behaves like a semi-strong typedef over std::string.
template <class TTag>
class TStrongIdentifierTypedef
{
public:
    TStrongIdentifierTypedef();

    TStrongIdentifierTypedef(const TStrongIdentifierTypedef&) = default;
    TStrongIdentifierTypedef(TStrongIdentifierTypedef&&) noexcept;

    TStrongIdentifierTypedef& operator=(const TStrongIdentifierTypedef&) = default;
    TStrongIdentifierTypedef& operator=(TStrongIdentifierTypedef&&) noexcept;

    template <size_t N>
    TStrongIdentifierTypedef(const char (&literal)[N]);

    explicit TStrongIdentifierTypedef(std::string_view str);

    constexpr explicit operator std::string_view() const noexcept;

    constexpr std::string_view Underlying() const& noexcept;

    size_t Capacity() const noexcept;

    template <typename TArgument>
    bool operator==(TArgument&& argument) const
        requires requires(const std::string& v, TArgument&& argument) { v == argument; };

    bool operator<(const TStrongIdentifierTypedef& rhs) const;
    bool operator>(const TStrongIdentifierTypedef& rhs) const;
    bool operator<=(const TStrongIdentifierTypedef& rhs) const;
    bool operator>=(const TStrongIdentifierTypedef& rhs) const;
    bool operator==(const TStrongIdentifierTypedef& rhs) const;
    bool operator!=(const TStrongIdentifierTypedef& rhs) const;

    void Save(IOutputStream* out) const;
    void Load(IInputStream* in);

private:
    ::TIntrusivePtr<TIdentifierStringData> Value_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

#define YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(T)                  \
    struct T##Tag                                             \
    { };                                                      \
    using T = ::NYT::NFlow::TStrongIdentifierTypedef<T##Tag>; \
    static_assert(true)

////////////////////////////////////////////////////////////////////////////////

#define IDENTIFIER_INL_H_
#include "identifier-inl.h"
#undef IDENTIFIER_INL_H_
