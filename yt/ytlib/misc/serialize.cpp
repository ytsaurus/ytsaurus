#include "stdafx.h"
#include "serialize.h"
#include <ytlib/logging/log.h>

namespace NYT {

static NLog::TLogger Logger("Serialize");

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

    TSharedRef result(size);
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

    i32 refCount;
    ReadPod(input, refCount);
    YCHECK(refCount >= 0);

    *refs = std::vector<TSharedRef>(refCount);
    for (i32 i = 0; i < refCount; ++i) {
        i64 refSize;
        ReadPod(input, refSize);
        TRef ref(const_cast<char*>(input.Buf()), static_cast<size_t>(refSize));
        (*refs)[i] = TSharedRef(packedRef, ref);
        input.Skip(refSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

