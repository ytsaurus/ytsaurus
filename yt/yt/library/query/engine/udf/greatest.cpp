#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <util/system/types.h>

#include <algorithm>
#include <cstring>

#define FIND_GREATEST(type, cmp) \
for (int i = 0; i < args_len; ++i) { \
    if (args[i].Type == NYT::NQueryClient::NUdf::EValueType::Null) { \
        ThrowException("Found null agrument"); \
        break; \
    } \
    if (cmp(args[greatestIdx].Data.type, args[i].Data.type, args[greatestIdx].Length, args[i].Length)) { \
        greatestIdx = i; \
    } \
} \
if (args_len == 0 || cmp(args[greatestIdx].Data.type, first->Data.type, args[greatestIdx].Length, first->Length)) { \
    greatestIdx = -1; \
} \

void copyString(TExpressionContext* context, TUnversionedValue* result, const char* source, int length)
{
    result->Length = length;
    result->Data.String = AllocateBytes(context, length);
    memcpy(result->Data.String, source, length);
}

extern "C" void greatest(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* first,
    TUnversionedValue* args,
    int args_len)
{
    result->Type = first->Type;
    int greatestIdx = 0;

    switch (first->Type) {
        case NYT::NQueryClient::NUdf::EValueType::Uint64:
            FIND_GREATEST(Uint64, [] (ui64 lhs, ui64 rhs, size_t /*lhsLen*/, size_t /*rhsLent*/) { return lhs < rhs; });
            break;
        case NYT::NQueryClient::NUdf::EValueType::Int64:
            FIND_GREATEST(Int64, [] (i64 lhs, i64 rhs, size_t /*lhsLen*/, size_t /*rhsLent*/) { return lhs < rhs; });
            break;
        case NYT::NQueryClient::NUdf::EValueType::Double:
            FIND_GREATEST(Double, [] (double lhs, double rhs, size_t /*lhsLen*/, size_t /*rhsLent*/) { return lhs < rhs; });
            break;
        case NYT::NQueryClient::NUdf::EValueType::Boolean:
            FIND_GREATEST(Boolean, [] (bool lhs, bool rhs, size_t /*lhsLen*/, size_t /*rhsLen*/) { return lhs < rhs; });
            break;
        case NYT::NQueryClient::NUdf::EValueType::String:
            FIND_GREATEST(
                String,
                [] (const char* lhs, const char* rhs, size_t lhsLen, size_t rhsLen)
                {
                    int cmpRes = ::memcmp(lhs, rhs, std::min(lhsLen, rhsLen));
                    if (cmpRes == 0) {
                        if (lhsLen < rhsLen) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                    return cmpRes < 0;
                });
            break;
        case NYT::NQueryClient::NUdf::EValueType::Null:
            ThrowException("Found null agrument");
            break;
        default:
            break;
    }

    switch (first->Type) {
        case NYT::NQueryClient::NUdf::EValueType::String:
            if (greatestIdx == -1) {
                copyString(context, result, first->Data.String, first->Length);
            } else {
                copyString(context, result, args[greatestIdx].Data.String, args[greatestIdx].Length);
            }
            break;
        default:
            if (greatestIdx == -1) {
                result->Data = first->Data;
            } else {
                result->Data = args[greatestIdx].Data;
            }
            break;
    }
}
