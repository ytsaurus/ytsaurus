#include "config.h"
#include "helpers.h"
#include "mapper.h"
#include "record_consumer.h"
#include "reducer.h"
#include "yt_connector.h"

#include <yt/yt/library/program/program.h>

#include <yt/cpp/mapreduce/interface/init.h>

namespace NYT::NSequoiaReconstructor {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_
            .AddLongOption(
                "single-cell",
                "Runs sequoia reconstructor for single cell with file output\n"
                "Expects config (in YSON format) for single cell reconstructor")
            .StoreResult(&SingleCellReconstructorConfig_)
            .Handler0([&] { RunSingleCell_ = true; })
            .RequiredArgument("CONFIG");
        Opts_
            .AddLongOption(
                "map-reduce",
                "Runs multicell sequoia reconstructor with map and reduce, outputting result into tables\n"
                "Expects config (in YSON format) for map reduce reconstructor")
            .StoreResult(&MapReduceReconstructorConfig_)
            .Handler0([&] { RunMapReduce_ = true; })
            .RequiredArgument("CONFIG");
    }

private:
    std::string SingleCellReconstructorConfig_;
    bool RunSingleCell_ = false;

    std::string MapReduceReconstructorConfig_;
    bool RunMapReduce_ = false;

    void DoRun() override
    {
        if (static_cast<int>(RunSingleCell_) + static_cast<int>(RunMapReduce_) != 1) {
            THROW_ERROR_EXCEPTION("Exactly one of --single-cell or --map-reduce must be specified");
        }

        if (RunSingleCell_) {
            RunSingleCell();
        }
        if (RunMapReduce_) {
            auto config = ConvertTo<TMulticellMapReduceSequoiaReconstructorConfigPtr>(TYsonString(MapReduceReconstructorConfig_));
            RunMapReduceReconstructor(config);
        }
    }

    void RunSingleCell()
    {
        auto config = ConvertTo<TSingleCellSequoiaReconstructorConfigPtr>(TYsonString(SingleCellReconstructorConfig_));

        RunWithBootstrap(
            config->MasterConfigPath,
            config->SnapshotPath,
            [&] (NCellMaster::TBootstrap* bootstrap) {
                auto consumer = CreateSingleCellRecordsConsumer(config);

                ExecuteSequoiaReconstructorMap(
                    bootstrap,
                    &consumer,
                    /*jobCount*/ 1,
                    /*jobIndex*/ 0);

                ExecutePathToNodeReduce(std::move(*consumer.PathToNodeChangeRecords), &consumer);
            },
            /*abortProcessAfterFinish*/ true);
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunSequoiaReconstructorProgram(int argc, const char** argv)
{
    Initialize(argc, argv);
    TProgram().Run(argc, argv);
}

} // namespace NYT::NSequoiaReconstructor
