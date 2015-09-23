#include "public.h"

#include <functional>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

// NB: Rows are allocated in row merger buffer which is cleared on each Read() call.

extern const int MinConcurrentReaders;

using TOverlappingReaderKeyComparer = std::function<int(
    const TUnversionedValue*,
    const TUnversionedValue*,
    const TUnversionedValue*,
    const TUnversionedValue*)>;

ISchemafulReaderPtr CreateSchemafulOverlappingLookupChunkReader(
    TSchemafulRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory);

ISchemafulReaderPtr CreateSchemafulOverlappingRangeChunkReader(
    const std::vector<TOwningKey>& boundaries,
    TSchemafulRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    const TOverlappingReaderKeyComparer& keyComparer,
    int minSimultaneousReaders = MinConcurrentReaders);

IVersionedReaderPtr CreateVersionedOverlappingRangeChunkReader(
    const std::vector<TOwningKey>& boundaries,
    TVersionedRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    const TOverlappingReaderKeyComparer& keyComparer,
    int minSimultaneousReaders = MinConcurrentReaders);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
