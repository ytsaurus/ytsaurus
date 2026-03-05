#include "printers.h"

#include <yt/yt/server/lib/hydra/serialize.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/public.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

namespace NYT::NTools::NDumpChangelog {

using namespace NYT::NYson;
using namespace NYT::NHydra;

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_
            .AddLongOption("input", "path to journal in YSON format")
            .StoreResult(&InputFile_)
            .Required();

        Opts_
            .AddLongOption("truncate-limit", "max string length to display fully")
            .StoreResult(&TruncateLimit_)
            .DefaultValue(30);

        Opts_
            .AddLongOption("no-truncate-strings", "do not truncate long strings")
            .StoreResult(&TruncateStrings_);
    }

private:
    TString InputFile_;
    bool TruncateStrings_ = true;
    int TruncateLimit_ = 30;

    void DoRun() override
    {
        TCustomPrinter::SetTruncateStrings(TruncateStrings_);
        TCustomPrinter::SetTruncateLimit(TruncateLimit_);

        TFileInput input(InputFile_);
        TYsonPullParser parser(&input, EYsonType::ListFragment);
        TYsonPullParserCursor cursor(&parser);
        YT_VERIFY(cursor.TryConsumeFragmentStart());
        static const TString PayloadKey("payload");
        int index = 0;
        while (!cursor->IsEndOfStream()) {
            cursor.ParseMap([&] (TYsonPullParserCursor* cursor) {
                auto valueType = (*cursor)->GetType();
                if (valueType != EYsonItemType::StringValue) {
                    THROW_ERROR_EXCEPTION(
                        "Unexpected value type: %Qv, expected: %Qv",
                        valueType,
                        EYsonItemType::StringValue);
                }

                if (auto key = (*cursor)->UncheckedAsString(); key != PayloadKey) {
                    THROW_ERROR_EXCEPTION("Expected %Qv but got %Qv while parsing changelog from YSON",
                        PayloadKey,
                        key);
                }
                cursor->Next();

                auto payload = ExtractTo<TString>(cursor);
                auto recordData = TSharedRef::FromString(std::move(payload));
                NHydra::NProto::TMutationHeader mutationHeader;
                TSharedRef mutationData;
                DeserializeMutationRecord(recordData, &mutationHeader, &mutationData);

                Cout << Format("Record %v", index) << Endl;
                Cout << Format("  MutationType:   %v", mutationHeader.mutation_type()) << Endl;
                Cout << Format("  Timestamp:      %v (%v)",
                    FromProto<TInstant>(mutationHeader.timestamp()),
                    mutationHeader.timestamp()) << Endl;
                Cout << Format("  RandomSeed:     %x", mutationHeader.random_seed()) << Endl;
                Cout << Format("  PrevRandomSeed: %x", mutationHeader.prev_random_seed()) << Endl;
                Cout << Format("  SegmentId:      %v", mutationHeader.segment_id()) << Endl;
                Cout << Format("  RecordId:       %v", mutationHeader.record_id()) << Endl;
                Cout << Format("  MutationId:     %v", FromProto<NRpc::TMutationId>(mutationHeader.mutation_id())) << Endl;
                Cout << Format("  Reign:          %v", mutationHeader.reign()) << Endl;
                Cout << Format("  SequenceNumber: %v", mutationHeader.sequence_number()) << Endl;
                Cout << Format("  Term:           %v", mutationHeader.term()) << Endl;

                Cout << PrintMutationContent(mutationHeader.mutation_type(), mutationData) << Endl;

                ++index;
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NDumpChangelog

int main(int argc, const char** argv)
{
    return NYT::NTools::NDumpChangelog::TProgram().Run(argc, argv);
}
