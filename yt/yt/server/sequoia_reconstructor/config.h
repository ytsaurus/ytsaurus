#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTableOutputConfig);

struct TTableOutputConfig
    : public NYTree::TYsonStruct
{
    std::string FileName;
    bool TextYsonOutputFormat;
    // TODO(grphil): Add other output options.

    REGISTER_YSON_STRUCT(TTableOutputConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableOutputConfig);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSequoiaReconstructorConfig);

struct TSequoiaReconstructorConfig
    : public NYTree::TYsonStruct
{
    bool SingleCellSingleRun;

    std::optional<TTableOutputConfigPtr> NodeIdToPathOutput;
    std::optional<TTableOutputConfigPtr> NodeForksOutput;
    std::optional<TTableOutputConfigPtr> NodeSnapshotsOutput;

    std::optional<TTableOutputConfigPtr> ChildNodesOutput;
    std::optional<TTableOutputConfigPtr> ChildForksOutput;

    std::optional<TTableOutputConfigPtr> PathToNodeIdOutput;
    std::optional<TTableOutputConfigPtr> PathForksOutput;

    std::optional<TTableOutputConfigPtr> PathToNodeChangesOutput;

    std::optional<TTableOutputConfigPtr> AclsOutput;

    std::optional<TTableOutputConfigPtr> TransactionsOutput;
    std::optional<TTableOutputConfigPtr> TransactionDescendantsOutput;
    std::optional<TTableOutputConfigPtr> DependentTransactionsOutput;
    std::optional<TTableOutputConfigPtr> TransactionReplicasOutput;

    REGISTER_YSON_STRUCT(TSequoiaReconstructorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSequoiaReconstructorConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
