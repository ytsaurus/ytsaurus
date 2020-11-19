#pragma once

#ifndef SKIFF_YSON_CONVERTER_INL_H_
#error "Direct inclusion of this file is not allowed; include skiff_yson_converter.h"
#endif

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType wireType>
Y_FORCE_INLINE auto TSimpleSkiffParser<wireType>::operator () (NSkiff::TCheckedInDebugSkiffParser* parser) const
{
    using namespace NSkiff;

    if constexpr (wireType == EWireType::Int64) {
        return parser->ParseInt64();
    } else if constexpr (wireType == EWireType::Uint64) {
        return parser->ParseUint64();
    } else if constexpr (wireType == EWireType::Boolean) {
        return parser->ParseBoolean();
    } else if constexpr (wireType == EWireType::Double) {
        return parser->ParseDouble();
    } else if constexpr (wireType == EWireType::String32) {
        return parser->ParseString32();
    } else if constexpr (wireType == EWireType::Nothing) {
        return nullptr;
    } else {
        static_assert(wireType == EWireType::Int64);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType SkiffWireType>
TDecimalSkiffParser<SkiffWireType>::TDecimalSkiffParser(int precision)
    : Precision_(precision)
{
    CheckSkiffWireTypeForDecimal(precision, SkiffWireType);
}

template <NSkiff::EWireType SkiffWireType>
Y_FORCE_INLINE TStringBuf TDecimalSkiffParser<SkiffWireType>::operator() (NSkiff::TCheckedInDebugSkiffParser* parser) const
{
    using namespace NSkiff;
    using namespace NDecimal;

    if constexpr (SkiffWireType == EWireType::Int32) {
        auto value = parser->ParseInt32();
        return TDecimal::WriteBinary32(Precision_, value, Buffer_, sizeof(Buffer_));
    } else if constexpr (SkiffWireType == EWireType::Int64) {
        auto value = parser->ParseInt64();
        return TDecimal::WriteBinary64(Precision_, value, Buffer_, sizeof(Buffer_));
    } else if constexpr (SkiffWireType == EWireType::Int128) {
        const auto skiffValue = parser->ParseInt128();
        return TDecimal::WriteBinary128(
            Precision_,
            TDecimal::TValue128{skiffValue.Low, skiffValue.High},
            Buffer_,
            sizeof(Buffer_));
    } else {
        static_assert(SkiffWireType == EWireType::Int128);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <NSkiff::EWireType SkiffWireType>
TDecimalSkiffWriter<SkiffWireType>::TDecimalSkiffWriter(int precision)
    : Precision_(precision)
{
    CheckSkiffWireTypeForDecimal(precision, SkiffWireType);
}

template <NSkiff::EWireType SkiffWireType>
void TDecimalSkiffWriter<SkiffWireType>::operator()(TStringBuf value, NSkiff::TCheckedInDebugSkiffWriter* writer) const
{
    using namespace NSkiff;
    using namespace NDecimal;

    if constexpr (SkiffWireType == EWireType::Int32) {
        auto intValue = TDecimal::ParseBinary32(Precision_, value);
        writer->WriteInt32(intValue);
    } else if constexpr (SkiffWireType == EWireType::Int64) {
        auto intValue = TDecimal::ParseBinary64(Precision_, value);
        writer->WriteInt64(intValue);
    } else if constexpr (SkiffWireType == EWireType::Int128) {
        auto intValue = TDecimal::ParseBinary128(Precision_, value);
        writer->WriteInt128(TInt128{intValue.Low, intValue.High});
    } else {
        // poor man's static_assert(false)
        static_assert(SkiffWireType == EWireType::Int128);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
