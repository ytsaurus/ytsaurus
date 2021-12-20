#pragma once

#include "public.h"

#include <utility>
#include <optional>
#include <typeindex>

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <yt/yt/core/misc/enum.h>

#include <library/cpp/yt/string/string_builder.h>

namespace NYT::NDI {

////////////////////////////////////////////////////////////////////////////////

//! TEnumValueIndex is like type_index for enum values.
struct TEnumValueIndex
{
    std::type_index EnumType;
    TString EnumName;
    TString ValueName;

    template <class TEnum>
    static TEnumValueIndex Get(TEnum kind)
    {
        return TEnumValueIndex{
            .EnumType = std::type_index(typeid(TEnum)),
            .EnumName = TString{TEnumTraits<TEnum>::GetTypeName()},
            .ValueName = ToString(kind),
        };
    }

    bool operator == (const TEnumValueIndex& other) const = default;
    operator size_t() const;
};

void FormatValue(TStringBuilderBase* builder, const TEnumValueIndex& index, TStringBuf /*spec*/);

TString ToString(const TEnumValueIndex& index);

////////////////////////////////////////////////////////////////////////////////

//! TObjectId identifies object within dependency graph.
struct TObjectId
{
    std::type_index Type;
    std::optional<TEnumValueIndex> Kind;
    std::optional<TString> Name;

    bool operator == (const TObjectId& other) const = default;
    operator size_t() const;
    TString TypeName() const;

    explicit TObjectId(std::type_index index)
        : Type(index)
    { }

    TObjectId GetTypeId() const;

    TObjectId CastTo(TObjectId other) const;

    template <class T>
    static TObjectId Get()
    {
        TObjectId id{std::type_index(typeid(T))};
        return id;
    }

    template <class T, class TEnum>
    static TObjectId Get(TEnum kind)
    {
        TObjectId id{std::type_index(typeid(T))};
        id.Kind = TEnumValueIndex::Get(kind);
        return id;
    }

    template <class T>
    static TObjectId GetNamed(const TString& name)
    {
        TObjectId id{std::type_index(typeid(T))};
        id.Name = name;
        return id;
    }
};

void FormatValue(TStringBuilderBase* builder, const TObjectId& id, TStringBuf /*spec*/);

TString ToString(const TObjectId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDI
