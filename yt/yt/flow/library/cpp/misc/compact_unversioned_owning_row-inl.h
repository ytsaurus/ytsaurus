#pragma once

#include "compact_unversioned_owning_row.h"

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TFunctor>
TCompactUnversionedOwningRow::TCompactUnversionedOwningRow(
    int count,
    size_t stringDataSize,
    TFunctor&& functor)
{
    // Allocate storage and initialize the row header.
    auto mutableRow = Preallocate(count, stringDataSize);

    // Call functor with a mutable view — values may point to external string data.
    functor(mutableRow);

    // Copy string data into the tail of the allocation and patch pointers.
    CopyStringData(count, stringDataSize);
}

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
    requires(sizeof...(Ts) > 0 && (... && std::same_as<std::remove_cvref_t<Ts>, NTableClient::TUnversionedValue>))
TCompactUnversionedOwningRow MakeCompactUnversionedOwningRow(Ts&&... values)
{
    // Compute total string data size via fold expression.
    return TCompactUnversionedOwningRow(
        static_cast<int>(sizeof...(Ts)),
        ((NTableClient::IsStringLikeType(values.Type) ? values.Length : 0) + ...),
        [&] (NTableClient::TMutableUnversionedRow row) {
            int i = 0;
            // Expand pack: assign each value into the pre-allocated row.
            ((row[i++] = std::forward<Ts>(values)), ...);
        });
}

template <class... Ts>
    requires(sizeof...(Ts) == 0 || !(... && std::same_as<std::remove_cvref_t<Ts>, NTableClient::TUnversionedValue>))
TCompactUnversionedOwningRow MakeCompactUnversionedOwningRow(Ts&&... values)
{
    NTableClient::TRowValueTypesChecker<Ts...>();
    constexpr bool AllTypesInline = (... && NTableClient::TUnversionedValueConversionTraits<Ts>::Inline);
    if constexpr (AllTypesInline) {
        return TCompactUnversionedOwningRow(
            static_cast<int>(sizeof...(Ts)),
            // All types are inline (no string data).
            /*stringDataSize*/ 0,
            [&] (NTableClient::TMutableUnversionedRow row) {
                int i = 0;
                int id = 0;
                ((row[i++] = NTableClient::ToUnversionedValue(std::forward<Ts>(values), /*rowBuffer*/ nullptr, id++)), ...);
            });
    } else {
        // Some types may produce string data: convert via a reusable thread-local row buffer.
        // No fiber context switch happens here, so a simple thread-local is safe.
        static thread_local NTableClient::TRowBufferPtr RowBuffer = New<NTableClient::TRowBuffer>();
        RowBuffer->Clear();
        int id = 0;
        NTableClient::TUnversionedValue arr[] = {NTableClient::ToUnversionedValue(std::forward<Ts>(values), RowBuffer, id++)...};
        return TCompactUnversionedOwningRow(NTableClient::TUnversionedValueRange(arr, sizeof...(Ts)));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
