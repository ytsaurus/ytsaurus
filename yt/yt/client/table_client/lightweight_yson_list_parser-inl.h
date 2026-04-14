#ifndef LIGHTWEIGHT_YSON_LIST_PARSER_INL_H_
#error "Direct inclusion of this file is not allowed, include lightweight_yson_list_parser.h"
// For the sake of sane code completion.
#include "lightweight_yson_list_parser.h"
#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class OnValue>
void DoUnpackValuesTyped(
    OnValue onValue,
    TStringBuf data,
    EValueType type,
    NYson::TStatelessLexer* lexer,
    NYson::TToken* token)
{
    using namespace NYson::NDetail;
    using NYson::ETokenType;

    const char* ptr = data.begin();
    const char* ptrEnd = data.end();

    if (ptr == ptrEnd) {
        THROW_ERROR_EXCEPTION("Unexpected end of data");
    }

    if (*ptr != BeginListSymbol) {
        THROW_ERROR_EXCEPTION("Expected begin of list");
    }

    ++ptr;

    auto parseList = [&] (auto onItem) {
        while (true) {
            if (ptr == ptrEnd) {
                THROW_ERROR_EXCEPTION("Unexpected end of data");
            }

            if (*ptr == EndListSymbol) {
                ++ptr;
                break;
            } if (*ptr == EntitySymbol) {
                ++ptr;

                if (onValue(MakeUnversionedNullValue())) {
                    break;
                }
            } else {
                auto value = onItem();

                if (onValue(value)) {
                    break;
                }

                if (ptr == ptrEnd) {
                    THROW_ERROR_EXCEPTION("Unexpected end of data");
                }
            }

            if (*ptr == ItemSeparatorSymbol) {
                ++ptr;
            } else if (*ptr == EndListSymbol) {
                ++ptr;
                break;
            } else {
                THROW_ERROR_EXCEPTION("Expected item separator");
            }
        }
    };

    switch (type) {
        case EValueType::Int64:
            parseList([&] {
                if (*ptr == Int64Marker) {
                    ++ptr;
                    ui64 uvalue;
                    ptr += ReadVarUint64(ptr, ptrEnd, &uvalue);
                    auto value = ZigZagDecode64(uvalue);
                    return MakeUnversionedInt64Value(value);
                }

                if (lexer) {
                    ptr += lexer->ParseToken(TStringBuf(ptr, ptrEnd), token);
                    if (token->GetType() == ETokenType::Int64) {
                        return MakeUnversionedInt64Value(token->GetInt64Value());
                    }
                }

                THROW_ERROR_EXCEPTION("Unexpected symbol. Expected EndListSymbol, Int64Marker or EntitySymbol");
            });
            break;
        case EValueType::Uint64:
            parseList([&] {
                if (*ptr == Uint64Marker) {
                    ++ptr;
                    ui64 uvalue;
                    ptr += ReadVarUint64(ptr, ptrEnd, &uvalue);
                    return MakeUnversionedUint64Value(uvalue);
                }

                if (lexer) {
                    ptr += lexer->ParseToken(TStringBuf(ptr, ptrEnd), token);
                    if (token->GetType() == ETokenType::Uint64) {
                        return MakeUnversionedUint64Value(token->GetUint64Value());
                    }
                }

                THROW_ERROR_EXCEPTION("Unexpected symbol. Expected EndListSymbol, Uint64Marker or EntitySymbol");
            });
            break;
        case EValueType::Double:
            parseList([&] {
                if (*ptr == DoubleMarker) {
                    ++ptr;

                    if (ptr + sizeof(double) > ptrEnd) {
                        THROW_ERROR_EXCEPTION("Unexpected end of data");
                    }

                    double value = *reinterpret_cast<const double*>(ptr);
                    ptr += sizeof(double);
                    return MakeUnversionedDoubleValue(value);
                }

                if (lexer) {
                    ptr += lexer->ParseToken(TStringBuf(ptr, ptrEnd), token);
                    if (token->GetType() == ETokenType::Double) {
                        return MakeUnversionedDoubleValue(token->GetDoubleValue());
                    }
                }

                THROW_ERROR_EXCEPTION("Unexpected symbol. Expected EndListSymbol, DoubleMarker or EntitySymbol");
            });
            break;
        case EValueType::Boolean:
            parseList([&] {
                if (*ptr == TrueMarker) {
                    ++ptr;
                    return MakeUnversionedBooleanValue(true);
                } else if (*ptr == FalseMarker) {
                    ++ptr;
                    return MakeUnversionedBooleanValue(false);
                }

                if (lexer) {
                    ptr += lexer->ParseToken(TStringBuf(ptr, ptrEnd), token);
                    if (token->GetType() == ETokenType::Boolean) {
                        return MakeUnversionedBooleanValue(token->GetBooleanValue());
                    }
                }

                THROW_ERROR_EXCEPTION("Unexpected symbol. Expected EndListSymbol, TrueMarker, FalseMarker or EntitySymbol");
            });
            break;
        case EValueType::String:
            parseList([&] {
                if (*ptr == StringMarker) {
                    ++ptr;

                    ui64 ulength;
                    ptr += ReadVarUint64(ptr, ptrEnd, &ulength);

                    i64 length = ZigZagDecode64(ulength);
                    if (length < 0) {
                        THROW_ERROR_EXCEPTION("Negative binary string literal length %v",
                            length);
                    }

                    if (ptr + length > ptrEnd) {
                        THROW_ERROR_EXCEPTION("Unexpected end of data");
                    }

                    auto value = TStringBuf(ptr, ptr + length);
                    ptr += length;
                    return MakeUnversionedStringValue(value);
                }

                if (lexer) {
                    ptr += lexer->ParseToken(TStringBuf(ptr, ptrEnd), token);
                    if (token->GetType() == ETokenType::String) {
                        return MakeUnversionedStringValue(token->GetStringValue());
                    }
                }

                THROW_ERROR_EXCEPTION("Unexpected symbol. Expected EndListSymbol, DoubleMarker or EntitySymbol");
            });
            break;
        default:
            THROW_ERROR_EXCEPTION("Unexpected type %Qlv",
                type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
