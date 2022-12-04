#include <yt/yt/library/program/program.h>

#include <yt/yt/server/lib/hydra_common/local_snapshot_store.h>

#include <util/stream/file.h>

namespace NYT::NHydra::NTools::NUnpackLocalSnapshot {

using namespace NHydra;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_
            .AddLongOption("input", "path to input snapshot file")
            .StoreResult(&InputFile_)
            .Required();
        Opts_
            .AddLongOption("output", "path to output snapshot file")
            .StoreResult(&OutputFile_)
            .Required();
    }

private:
    TString InputFile_;
    TString OutputFile_;


    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        auto reader = CreateLocalSnapshotReader(InputFile_, InvalidSegmentId);

        WaitFor(reader->Open())
            .ThrowOnError();

        TFileOutput output(OutputFile_);

        while (true) {
            auto block = WaitFor(reader->Read())
                .ValueOrThrow();

            if (!block) {
                break;
            }

            output.Write(block.Begin(), block.Size());
        }

        output.Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra::NTools::NUnpackLocalSnapshot

int main(int argc, const char** argv)
{
    return NYT::NHydra::NTools::NUnpackLocalSnapshot::TProgram().Run(argc, argv);
}
