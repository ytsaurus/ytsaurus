#include "core_dump.h"

#include <yt/core/misc/size_literals.h>
#include <yt/core/misc/assert.h>

#include <array>

namespace NYT {
namespace NCoreDump {

////////////////////////////////////////////////////////////////////////////////

i64 WriteSparseCoreDump(IInputStream* in, TFile* out)
{
    typedef std::array<char, 4_KB> Page;
    constexpr int BatchSize = 128;
    
    Page zeroPage = {};
    i64 offset = 0;
    std::vector<Page> pages(BatchSize);
    while (true) {
        auto size = in->Load(pages[0].data(), sizeof(Page) * BatchSize);
        if (size == 0) {
            break;
        }

        if (size % sizeof(Page) != 0) {
            out->Pwrite(pages[0].data(), size, offset);
            offset += size;
            break;
        }

        int start = 0;
        auto flushPage = [&] (int end) {
            auto n = end - start;
            if (n != 0) {
                out->Pwrite(pages[start].data(), sizeof(Page) * n, offset);
            }

            start = end;
            offset += sizeof(Page) * n;
        };

        int i = 0;
        for (; i * sizeof(Page) < size; ++i) {
            if (pages[i] == zeroPage) {
                flushPage(i);

                start++;
                offset += sizeof(Page);
            }
        }
        flushPage(i);
    }

    out->Resize(offset);
    return offset;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
