#include "config.h"

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void TTableOutputConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("file_name", &TThis::FileName)
        .IsRequired();
    registrar.Parameter("text_yson_output_format", &TThis::TextYsonOutputFormat)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSequoiaReconstructorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("single_cell_single_run", &TThis::SingleCellSingleRun)
        .Default(false);

    registrar.Parameter("node_id_to_path_output", &TThis::NodeIdToPathOutput)
        .Default();

    registrar.Parameter("node_forks_output", &TThis::NodeForksOutput)
        .Default();

    registrar.Parameter("node_snapshots_output", &TThis::NodeSnapshotsOutput)
        .Default();

    registrar.Parameter("child_nodes_output", &TThis::ChildNodesOutput)
        .Default();

    registrar.Parameter("child_forks_output", &TThis::ChildForksOutput)
        .Default();

    registrar.Parameter("path_to_node_id_output", &TThis::PathToNodeIdOutput)
        .Default();

    registrar.Parameter("path_forks_output", &TThis::PathForksOutput)
        .Default();

    registrar.Parameter("path_to_node_changes_output", &TThis::PathToNodeChangesOutput)
        .Default();

    registrar.Parameter("acls_output", &TThis::AclsOutput)
        .Default();

    registrar.Parameter("transactions_output", &TThis::TransactionsOutput)
        .Default();

    registrar.Parameter("transaction_descendants_output", &TThis::TransactionDescendantsOutput)
        .Default();

    registrar.Parameter("dependent_transactions_output", &TThis::DependentTransactionsOutput)
        .Default();

    registrar.Parameter("transaction_replicas_output", &TThis::TransactionReplicasOutput)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->SingleCellSingleRun) {
            THROW_ERROR_EXCEPTION_IF(
                config->PathToNodeChangesOutput,
                "Single cell single run sequoia reconstructor will not construct \"path_to_node_changes_output\" table. "
                "This table is used only in separate map and reduce stages.");
        }
        // TODO(grphil): Add verifications for map and reduce configs.
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
