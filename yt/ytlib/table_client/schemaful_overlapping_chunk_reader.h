#include "public.h"

#include <functional>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

// NB: rows are allocated in row merger buffer which is cleared on each Read() call.

extern const int MinSimultaneousReaders;

using TOverlappingReaderKeyComparer = std::function<int(
    const TUnversionedValue*,
    const TUnversionedValue*,
    const TUnversionedValue*,
    const TUnversionedValue*)>;

ISchemafulReaderPtr CreateSchemafulOverlappingChunkLookupReader(
    TSchemafulRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory);

ISchemafulReaderPtr CreateSchemafulOverlappingChunkReader(
    const std::vector<TOwningKey>& boundaries,
    TSchemafulRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    const TOverlappingReaderKeyComparer& keyComparer,
    int minSimultaneousReaders = MinSimultaneousReaders);

IVersionedReaderPtr CreateVersionedOverlappingChunkReader(
    const std::vector<TOwningKey>& boundaries,
    TVersionedRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    const TOverlappingReaderKeyComparer& keyComparer,
    int minSimultaneousReaders = MinSimultaneousReaders);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
