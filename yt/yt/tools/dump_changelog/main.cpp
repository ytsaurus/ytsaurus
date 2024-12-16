#include <yt/yt/server/lib/hydra/serialize.h>

#include <yt/yt/ytlib/hive/proto/hive_service.pb.h>

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
    }

private:
    TString InputFile_;

    void DoRun() override
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

                PrintMutationContent(mutationHeader.mutation_type(), mutationData);

                Cout << Endl;

                ++index;
            });
        }
    }

    template <class TMutation>
    void PrintHiveMutation(const TMutation& mutation)
    {
        Cout << "  Messages:" << Endl;
        for (const auto& message : mutation.messages()) {
            Cout << "    Type: " << message.type() << Endl;
            PrintMutationContent(message.type(), TSharedRef::FromString(message.data()), /*offset*/ 4);
            Cout << "    --------------------" << Endl;
            Cout << Endl;
        }
    }

    void PrintMutationContent(TString type, TSharedRef data, int offset = 2)
    {
        TString offsetPrefix(offset, ' ');

        if (const auto* descriptor = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(type)) {
            const auto* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
            std::unique_ptr<google::protobuf::Message> message(prototype->New());
            DeserializeProtoWithEnvelope(message.get(), data);

            if (type == "NYT.NHiveClient.NProto.TReqPostMessages") {
                PrintHiveMutation(static_cast<NHiveClient::NProto::TReqPostMessages&>(*message));
            } else if (type == "NYT.NHiveClient.NProto.TReqSendMessages") {
                PrintHiveMutation(static_cast<NHiveClient::NProto::TReqPostMessages&>(*message));
            }

            auto parts = SplitString(message->DebugString(), "\n");
            for (const auto& part : parts) {
                Cout << offsetPrefix << part << "\n";
            }
        } else {
            Cout << offsetPrefix << "<Unknown protobuf type>" << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NDumpChangelog

int main(int argc, const char** argv)
{
    // Proto messages must be instantiated to be pretty-printed.
    Y_UNUSED(NYT::NHiveClient::NProto::TReqSendMessages{});
    Y_UNUSED(NYT::NHiveClient::NProto::TReqPostMessages{});

    return NYT::NTools::NDumpChangelog::TProgram().Run(argc, argv);
}
