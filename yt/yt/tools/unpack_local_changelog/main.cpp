#include <yt/yt/library/program/program.h>

#include <yt/yt/server/lib/hydra/config.h>
#include <yt/yt/server/lib/hydra/file_changelog.h>
#include <yt/yt/server/lib/hydra/file_changelog_dispatcher.h>

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/writer.h>

#include <util/stream/file.h>

namespace NYT::NTools::NUnpackLocalChangelog {

using namespace NHydra;
using namespace NIO;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_
            .AddLongOption("input", "path to input changelog file")
            .StoreResult(&InputFile_)
            .Required();
        Opts_
            .AddLongOption("output", "path to output changelog file")
            .StoreResult(&OutputFile_)
            .Required();
        Opts_
            .AddLongOption("from", "first record index")
            .StoreResult(&FirstRecordIndex_);
        Opts_
            .AddLongOption("to", "last record index")
            .StoreResult(&LastRecordIndex_);
    }

private:
    TString InputFile_;
    TString OutputFile_;
    std::optional<int> FirstRecordIndex_;
    std::optional<int> LastRecordIndex_;


    void DoRun() override
    {
        auto ioEngineConfigNode = GetEphemeralNodeFactory()->CreateMap();

        auto ioEngine = CreateIOEngine(
            EIOEngineType::ThreadPool,
            ioEngineConfigNode);

        auto changelogDispatcherConfig = New<TFileChangelogDispatcherConfig>();

        auto changelogDispatcher = CreateFileChangelogDispatcher(
            ioEngine,
            /*memoryUsageTracker*/ nullptr,
            changelogDispatcherConfig,
            "Dispatcher");

        auto changelogConfig = New<TFileChangelogConfig>();

        auto changelog = WaitFor(changelogDispatcher->OpenChangelog(InvalidSegmentId, InputFile_, changelogConfig))
            .ValueOrThrow();

        TFileOutput fileOutput(OutputFile_);
        TYsonWriter ysonWriter(&fileOutput, EYsonFormat::Binary, EYsonType::ListFragment);

        int recordIndex = FirstRecordIndex_.value_or(0);
        while (recordIndex < changelog->GetRecordCount()) {
            constexpr i64 MaxBytesPerRead = 16_MBs;
            auto records = WaitFor(changelog->Read(recordIndex, std::numeric_limits<int>::max(), MaxBytesPerRead))
                .ValueOrThrow();

            for (const auto& record : records) {
                if (LastRecordIndex_ && recordIndex > *LastRecordIndex_) {
                    break;
                }

                ++recordIndex;

                BuildYsonListFragmentFluently(&ysonWriter)
                    .Item().BeginMap()
                        .Item(NApi::JournalPayloadKey).Value(record.ToStringBuf())
                    .EndMap();
            }

            if (LastRecordIndex_ && recordIndex > *LastRecordIndex_) {
                break;
            }
        }

        ysonWriter.Flush();
        fileOutput.Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NUnpackLocalChangelog

int main(int argc, const char** argv)
{
    return NYT::NTools::NUnpackLocalChangelog::TProgram().Run(argc, argv);
}
