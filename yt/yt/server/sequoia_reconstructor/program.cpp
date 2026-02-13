#include "config.h"
#include "sequoia_reconstructor.h"

#include <yt/yt/library/program/program.h>

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
            .AddLongOption("master-config", "path to master config file")
            .StoreResult(&MasterConfigPath_)
            .Required();
        Opts_
            .AddLongOption("snapshot-path", "path to master snapshot")
            .StoreResult(&SnapshotPath_)
            .Required();
        Opts_
            .AddLongOption("reconstructor-config", "Sequoia reconstructor config")
            .StoreResult(&ReconstructorConfig_)
            .Required();
    }

private:
    std::string MasterConfigPath_;
    std::string SnapshotPath_;
    std::string ReconstructorConfig_;

    void DoRun() override
    {
        auto config = ConvertTo<TSequoiaReconstructorConfigPtr>(TYsonString(ReconstructorConfig_));

        if (config->SingleCellSingleRun) {
            ReconstructSequoia(config, MasterConfigPath_, SnapshotPath_);
        } else {
            // TODO(grphil): Implement map reduce reconstructor.
            THROW_ERROR_EXCEPTION("Not implemented");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunSequoiaReconstructorProgram(int argc, const char** argv)
{
    TProgram().Run(argc, argv);
}

} // namespace NYT::NSequoiaReconstructor
