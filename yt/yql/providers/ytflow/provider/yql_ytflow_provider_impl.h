#pragma once

#include "yql_ytflow_state.h"

#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>


namespace NYql {

THolder<TVisitorTransformerBase> CreateYtflowDataSourceTypeAnnotationTransformer(TYtflowState::TPtr state);
THolder<TVisitorTransformerBase> CreateYtflowDataSinkTypeAnnotationTransformer(TYtflowState::TPtr state);

THolder<IGraphTransformer> CreateYtflowDataSourceConstraintTransformer(TYtflowState::TPtr state);

THolder<IGraphTransformer> CreateYtflowRecaptureOptProposalTransformer(TYtflowState::TPtr state);

THolder<TExecTransformerBase> CreateYtflowDataSourceExecTransformer(TYtflowState::TPtr state);
THolder<TExecTransformerBase> CreateYtflowDataSinkExecTransformer(TYtflowState::TPtr state);

THolder<IGraphTransformer> CreateYtflowLogicalOptProposalTransformer(TYtflowState::TPtr state);
THolder<IGraphTransformer> CreateYtflowPhysicalOptProposalTransformer(TYtflowState::TPtr state);
THolder<IGraphTransformer> CreateYtflowPhysicalFinalizingTransformer(TYtflowState::TPtr state);

} // namespace NYql
