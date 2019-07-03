#include "helpers.h"

#include <yt/ytlib/core_dump/proto/core_info.pb.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <util/generic/size_literals.h>

namespace NYT::NCoreDump {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TCoreInfo& coreInfo, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("process_id").Value(coreInfo.process_id())
            .Item("executable_name").Value(coreInfo.executable_name())
            .DoIf(coreInfo.has_size(), [&] (TFluentMap fluent) {
                fluent
                    .Item("size").Value(coreInfo.size());
            })
            .DoIf(coreInfo.has_error(), [&] (TFluentMap fluent) {
                fluent
                    .Item("error").Value(NYT::FromProto<TError>(coreInfo.error()));
            })
        .EndMap();
}

} // namespace NProto

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

} // namespace NYT::NCoreDump
