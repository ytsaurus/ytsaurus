#ifndef POSITION_INDEPENDENT_VALUE_INL_H_
#error "Direct inclusion of this file is not allowed, include position_independent_value.h"
// For the sake of sane code completion.
#include "position_independent_value.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T FromPositionIndependentValue(const TPIValue& positionIndependentValue)
{
    TUnversionedValue asUnversioned;
    MakeUnversionedFromPositionIndependent(&asUnversioned, positionIndependentValue);
    return FromUnversionedValue<T>(asUnversioned);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE const char* GetStringPosition(const TPIValue& value)
{
    return reinterpret_cast<char*>(value.Data.StringOffset + reinterpret_cast<int64_t>(&value.Data.StringOffset));
}

Y_FORCE_INLINE void SetStringPosition(TPIValue* value, const char* string)
{
    value->Data.StringOffset = reinterpret_cast<int64_t>(string) - reinterpret_cast<int64_t>(&value->Data.StringOffset);
}

Y_FORCE_INLINE void TPIValue::SetStringPosition(const char* string)
{
    NQueryClient::SetStringPosition(this, string);
}

Y_FORCE_INLINE TStringBuf TPIValue::AsStringBuf() const
{
    return TStringBuf(GetStringPosition(*this), Length);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void MakeUnversionedFromPositionIndependent(TUnversionedValue* destination, const TPIValue& source)
{
    destination->Id = source.Id;
    destination->Type = source.Type;
    destination->Flags = source.Flags;
    destination->Length = source.Length;

    if (IsStringLikeType(source.Type)) {
        destination->Data.String = GetStringPosition(source);
    } else {
        destination->Data.Uint64 = source.Data.Uint64;
    }
}

Y_FORCE_INLINE void MakePositionIndependentFromUnversioned(TPIValue* destination, const TUnversionedValue& source)
{
    destination->Id = source.Id;
    destination->Type = source.Type;
    destination->Flags = source.Flags;
    destination->Length = source.Length;

    if (IsStringLikeType(source.Type)) {
        SetStringPosition(destination, source.Data.String);
    } else {
        destination->Data.Uint64 = source.Data.Uint64;
    }
}

Y_FORCE_INLINE void CopyPositionIndependent(TPIValue* destination, const TPIValue& source)
{
    destination->Id = source.Id;
    destination->Type = source.Type;
    destination->Flags = source.Flags;
    destination->Length = source.Length;

    if (IsStringLikeType(source.Type)) {
        SetStringPosition(destination, GetStringPosition(source));
    } else {
        destination->Data.Uint64 = source.Data.Uint64;
    }
}

////////////////////////////////////////////////////////////////////////////////

inline void MakePositionIndependentSentinelValue(TPIValue* result, EValueType type, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedSentinelValue(type, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

inline void MakePositionIndependentNullValue(TPIValue* result, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedNullValue(id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

inline void MakePositionIndependentInt64Value(TPIValue* result, i64 value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedInt64Value(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

inline void MakePositionIndependentUint64Value(TPIValue* result, ui64 value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedUint64Value(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

inline void MakePositionIndependentDoubleValue(TPIValue* result, double value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedDoubleValue(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

inline void MakePositionIndependentBooleanValue(TPIValue* result, bool value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedBooleanValue(value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

inline void MakePositionIndependentStringLikeValue(TPIValue* result, EValueType valueType, TStringBuf value, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedStringLikeValue(valueType, value, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

inline void MakePositionIndependentValueHeader(TPIValue* result, EValueType type, int id, EValueFlags flags)
{
    auto asUnversioned = MakeUnversionedValueHeader(type, id, flags);
    MakePositionIndependentFromUnversioned(result, asUnversioned);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
