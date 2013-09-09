#include "stdafx.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSharedRef PackRefs(const std::vector<TSharedRef>& refs)
{
    i64 size = 0;

    // Number of bytes to hold vector size.
    size += sizeof(i32);
    // Number of bytes to hold ref sizes.
    size += sizeof(i64) * refs.size();
    // Number of bytes to hold refs.
    FOREACH (const auto& ref, refs) {
        size += ref.Size();
    }

    struct TPackedRefsTag { };
    auto result = TSharedRef::Allocate<TPackedRefsTag>(size, false);
    TMemoryOutput output(result.Begin(), result.Size());

    WritePod(output, static_cast<i32>(refs.size()));
    FOREACH (const auto& ref, refs) {
        WritePod(output, static_cast<i64>(ref.Size()));
        Write(output, ref);
    }

    return result;
}

void UnpackRefs(const TSharedRef& packedRef, std::vector<TSharedRef>* refs)
{
    TMemoryInput input(packedRef.Begin(), packedRef.Size());

    i32 count;
    ::Load(&input, count);
    YCHECK(count >= 0);

    *refs = std::vector<TSharedRef>(count);
    for (int index = 0; index < count; ++index) {
        i64 refSize;
        ::Load(&input, refSize);
        TRef ref(const_cast<char*>(input.Buf()), static_cast<size_t>(refSize));
        (*refs)[index] = packedRef.Slice(ref);
        input.Skip(refSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

TStreamSaveContext::TStreamSaveContext()
    : Output_(nullptr)
{ }

TStreamLoadContext::TStreamLoadContext()
    : Input_(nullptr)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

