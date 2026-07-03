#include "yql_yt_phy_opt.h"

#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>

namespace NYql {

using namespace NNodes;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Alter(TExprBase node, TExprContext& ctx) const {
    const auto& alter = node.Cast<TYtAlterTable>();
    const TYtTableInfo tableInfo(alter.Table());
    const TYtOutTableInfo outTable(State_->TablesData->GetTable(alter.DataSink().Cluster().StringValue(), tableInfo.Name, tableInfo.CommitEpoch).RowSpec);

    return Build<TYtPublish>(ctx, alter.Pos())
        .World(alter.World())
        .DataSink(alter.DataSink())
        .Input()
            .Add<TYtOutput>()
                .Operation<TYtAlter>()
                    .World(alter.World())
                    .DataSink(alter.DataSink())
                    .Output()
                        .Add(outTable.ToExprNode(ctx, alter.Table().Pos()))
                        .Build()
                    .Input()
                        .Add()
                            .Paths()
                                .Add()
                                    .Table(alter.Table())
                                    .Columns<TCoVoid>().Build()
                                    .Ranges<TCoVoid>().Build()
                                    .Stat<TCoVoid>().Build()
                                    .QLFilter<TCoVoid>().Build()
                                    .Build()
                                .Build()
                            .Settings().Build()
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Build()
                .OutIndex().Value(0U).Build()
                .Build()
            .Build()
        .Publish(alter.Table())
        .Settings(alter.Settings())
        .Done();
}

}  // namespace NYql
