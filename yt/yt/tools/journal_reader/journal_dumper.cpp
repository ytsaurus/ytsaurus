#include "journal_dumper.h"

#include <yt/yt/server/lib/hydra/serialize.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/ytree/node.h>

namespace NYT {

using namespace NHydra::NProto;
using namespace NHydra;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TJournalDumper::Dump(const std::vector<IMapNodePtr>& records)
{
    for (const auto& ysonRecord : records) {
        auto stringNode = ysonRecord->AsMap()->GetChildOrThrow("data")->AsString();
        auto str = stringNode->GetValue();
        auto recordData = TSharedRef::FromString(str);

        TMutationHeader header;
        TSharedRef requestData;
        DeserializeMutationRecord(recordData, &header, &requestData);

        for (const auto& dumper : MutationDumpers_) {
            dumper->Dump(header, requestData);
        }
    }
}

void TJournalDumper::AddMutationDumper(IMutationDumperPtr dumper)
{
    MutationDumpers_.push_back(std::move(dumper));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
