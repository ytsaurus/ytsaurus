#include <yt/yt/server/lib/hydra/serialize.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/public.h>

#include <util/stream/file.h>

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
    }

private:
    TString InputFile_;

    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
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
                Cout << Format("  Timestamp:      %v", mutationHeader.timestamp()) << Endl;
                Cout << Format("  RandomSeed:     %x", mutationHeader.random_seed()) << Endl;
                Cout << Format("  PrevRandomSeed: %x", mutationHeader.prev_random_seed()) << Endl;
                Cout << Format("  SegmentId:      %v", mutationHeader.segment_id()) << Endl;
                Cout << Format("  RecordId:       %v", mutationHeader.record_id()) << Endl;
                Cout << Format("  MutationId:     %v", FromProto<NRpc::TMutationId>(mutationHeader.mutation_id())) << Endl;
                Cout << Format("  Reign:          %v", mutationHeader.reign()) << Endl;
                Cout << Format("  SequenceNumber: %v", mutationHeader.sequence_number()) << Endl;
                Cout << Format("  Term:           %v", mutationHeader.term()) << Endl;

                if (const auto* descriptor = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(mutationHeader.mutation_type())) {
                    const auto* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
                    std::unique_ptr<google::protobuf::Message> message(prototype->New());
                    DeserializeProtoWithEnvelope(message.get(), mutationData);
                    Cout << message->DebugString() << Endl;
                } else {
                    Cout << "<Unknown protobuf type>" << Endl;
                }

                Cout << Endl;

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
