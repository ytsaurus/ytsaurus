
#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <yt/yt/library/webassembly/api/type_builder.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

#define XX(signature, type) \
template <> \
EWebAssemblyValueType InferType< signature >() \
{ \
    return type; \
}

    XX(bool, EWebAssemblyValueType::Int32)
    XX(char, EWebAssemblyValueType::Int32)
    XX(int, EWebAssemblyValueType::Int32)
    XX(unsigned int, EWebAssemblyValueType::Int32)

    XX(long, EWebAssemblyValueType::Int64)
    XX(unsigned long, EWebAssemblyValueType::Int64)

    XX(float, EWebAssemblyValueType::Float32)
    XX(double, EWebAssemblyValueType::Float64)

    XX(char*, EWebAssemblyValueType::UintPtr)
    XX(const char*, EWebAssemblyValueType::UintPtr)
    XX(char**, EWebAssemblyValueType::UintPtr)
    XX(const uint8_t*, EWebAssemblyValueType::UintPtr)
    XX(const uint8_t**, EWebAssemblyValueType::UintPtr)
    XX(int*, EWebAssemblyValueType::UintPtr)
    XX(unsigned long*, EWebAssemblyValueType::UintPtr)
    XX(void*, EWebAssemblyValueType::UintPtr)
    XX(void**, EWebAssemblyValueType::UintPtr)
    XX(void* const*, EWebAssemblyValueType::UintPtr)
    XX(const void*, EWebAssemblyValueType::UintPtr)

    XX(NTableClient::TUnversionedValue*, EWebAssemblyValueType::UintPtr)
    XX(NTableClient::TUnversionedValue const*, EWebAssemblyValueType::UintPtr)
    XX(const NTableClient::TUnversionedValue**, EWebAssemblyValueType::UintPtr)

    XX(TSharedRange<NTableClient::TUnversionedRow>*, EWebAssemblyValueType::UintPtr)
    XX(TSharedRange<NQueryClient::TRowRange>*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TPositionIndependentValue*, EWebAssemblyValueType::UintPtr)
    XX(const NQueryClient::TPositionIndependentValue*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TPositionIndependentValue**, EWebAssemblyValueType::UintPtr)
    XX(const NQueryClient::TPositionIndependentValue**, EWebAssemblyValueType::UintPtr)

    XX(TSharedRange<TRange<NQueryClient::TPositionIndependentValue>>*, EWebAssemblyValueType::UintPtr)

    XX(NTableClient::TRowBuffer*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TFunctionContext*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TWriteOpClosure*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TGroupByClosure*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TTopCollector*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TMultiJoinClosure*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TMultiJoinParameters*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TJoinComparers*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TExecutionContext*, EWebAssemblyValueType::UintPtr)

    XX(bool(*)(void**, NTableClient::TRowBuffer*, const NQueryClient::TPositionIndependentValue**, long), EWebAssemblyValueType::UintPtr)
    XX(bool(*)(void**, NTableClient::TRowBuffer*, NTableClient::TUnversionedValue const**, long), EWebAssemblyValueType::UintPtr)
    XX(char(*)(const NQueryClient::TPositionIndependentValue*, const NQueryClient::TPositionIndependentValue*), EWebAssemblyValueType::UintPtr)
    XX(char(*)(NTableClient::TUnversionedValue const*, NTableClient::TUnversionedValue const*), EWebAssemblyValueType::UintPtr)
    XX(unsigned long(*)(const NQueryClient::TPositionIndependentValue*), EWebAssemblyValueType::UintPtr)
    XX(unsigned long(*)(NTableClient::TUnversionedValue const*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TGroupByClosure*, NTableClient::TRowBuffer*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TMultiJoinClosure*, NTableClient::TRowBuffer*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TTopCollector*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TWriteOpClosure*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NTableClient::TRowBuffer*), EWebAssemblyValueType::UintPtr)

    XX(const time_t*, EWebAssemblyValueType::UintPtr)
    XX(tm*, EWebAssemblyValueType::UintPtr)
    XX(const tm*, EWebAssemblyValueType::UintPtr)
    XX(re2::RE2*, EWebAssemblyValueType::UintPtr)

    using TPIRanges = TSharedRange<std::pair<
        TRange<NQueryClient::TPositionIndependentValue>,
        TRange<NQueryClient::TPositionIndependentValue>>>*;
    XX(TPIRanges, EWebAssemblyValueType::UintPtr)

    using TLookupTable = std::unique_ptr<google::dense_hash_set<
        const NQueryClient::TPositionIndependentValue*,
        NQueryClient::NDetail::TGroupHasher,
        NQueryClient::NDetail::TRowComparer,
        google::libc_allocator_with_realloc<const NQueryClient::TPositionIndependentValue *>>*>;
    XX(TLookupTable, EWebAssemblyValueType::UintPtr)

    XX(void, EWebAssemblyValueType::Void)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NWebAssembly
