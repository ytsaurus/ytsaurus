#include <yt/yt/library/query/base/public.h>
#include <yt/yt/library/query/base/query.h>

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <yt/yt/library/web_assembly/api/type_builder.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

#define XX(signature, type) \
    template <> \
    EWebAssemblyValueType InferType< signature >() \
    { \
        return type; \
    }

    XX(NTableClient::TUnversionedValue*, EWebAssemblyValueType::UintPtr)
    XX(NTableClient::TUnversionedValue**, EWebAssemblyValueType::UintPtr)
    XX(NTableClient::TUnversionedValue const*, EWebAssemblyValueType::UintPtr)
    XX(const NTableClient::TUnversionedValue**, EWebAssemblyValueType::UintPtr)

    XX(TSharedRange<NTableClient::TUnversionedRow>*, EWebAssemblyValueType::UintPtr)
    XX(TSharedRange<NQueryClient::TRowRange>*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::EStringMatchOp, EWebAssemblyValueType::Int32)

    XX(NQueryClient::TPositionIndependentValue*, EWebAssemblyValueType::UintPtr)
    XX(const NQueryClient::TPositionIndependentValue*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TPositionIndependentValue**, EWebAssemblyValueType::UintPtr)
    XX(const NQueryClient::TPositionIndependentValue**, EWebAssemblyValueType::UintPtr)

    XX(TSharedRange<TRange<NQueryClient::TPositionIndependentValue>>*, EWebAssemblyValueType::UintPtr)

    XX(NTableClient::TRowBuffer*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TExpressionContext*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TFunctionContext*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TRowSchemaInformation*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TWriteOpClosure*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TGroupByClosure*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TTopCollector*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TMultiJoinClosure*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TArrayJoinParameters*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TMultiJoinParameters*, EWebAssemblyValueType::UintPtr)
    XX(NQueryClient::TJoinComparers*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TLikeExpressionContext*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TExecutionContext*, EWebAssemblyValueType::UintPtr)

    XX(bool(*)(void**, NQueryClient::TExpressionContext*, const NQueryClient::TPositionIndependentValue**, long), EWebAssemblyValueType::UintPtr)
    XX(bool(*)(void**, NQueryClient::TExpressionContext*, NTableClient::TUnversionedValue const**, long), EWebAssemblyValueType::UintPtr)
    XX(char(*)(const NQueryClient::TPositionIndependentValue*, const NQueryClient::TPositionIndependentValue*), EWebAssemblyValueType::UintPtr)
    XX(char(*)(NTableClient::TUnversionedValue const*, NTableClient::TUnversionedValue const*), EWebAssemblyValueType::UintPtr)
    XX(unsigned long(*)(const NQueryClient::TPositionIndependentValue*), EWebAssemblyValueType::UintPtr)
    XX(unsigned long(*)(NTableClient::TUnversionedValue const*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TGroupByClosure*, NQueryClient::TExpressionContext*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TMultiJoinClosure*, NQueryClient::TExpressionContext*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TTopCollector*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TWriteOpClosure*), EWebAssemblyValueType::UintPtr)
    XX(void(*)(void**, NQueryClient::TExpressionContext*), EWebAssemblyValueType::UintPtr)
    XX(bool(*)(void**, NQueryClient::TExpressionContext*, const NQueryClient::TPositionIndependentValue*), EWebAssemblyValueType::UintPtr)

    XX(const time_t*, EWebAssemblyValueType::UintPtr)
    XX(struct tm*, EWebAssemblyValueType::UintPtr)
    XX(const struct tm*, EWebAssemblyValueType::UintPtr)
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

    XX(std::unique_ptr<NQueryClient::TLookupRowInRowsetWebAssemblyContext>*, EWebAssemblyValueType::UintPtr)

    XX(NQueryClient::TCompositeMemberAccessorPath*, EWebAssemblyValueType::UintPtr)
    XX(NTableClient::EValueType, EWebAssemblyValueType::Int32)

#undef XX

////////////////////////////////////////////////////////////////////////////////

} // namespace NWebAssembly
