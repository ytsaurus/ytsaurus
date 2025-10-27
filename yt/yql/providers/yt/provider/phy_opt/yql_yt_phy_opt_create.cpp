#include "yql_yt_phy_opt.h"

#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>

namespace NYql {

using namespace NNodes;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::Create(TExprBase node, TExprContext& ctx) const {
    const auto& create = node.Cast<TYtCreateTable>();
    const TYtTableInfo tableInfo(create.Table());
    const TYtOutTableInfo outTable(State_->TablesData->GetTable(create.DataSink().Cluster().StringValue(), tableInfo.Name, tableInfo.CommitEpoch).RowSpec);

    return Build<TYtPublish>(ctx, create.Pos())
        .World(create.World())
        .DataSink(create.DataSink())
        .Input()
            .Add<TYtOutput>()
                .Operation<TYtTouch>()
                    .World(create.World())
                    .DataSink(create.DataSink())
                    .Output()
                        .Add(outTable.ToExprNode(ctx, create.Table().Pos()))
                        .Build()
                    .Build()
                .OutIndex().Value(0U).Build()
                .Build()
            .Build()
        .Publish(create.Table())
        .Settings(create.Settings())
        .Done();
}

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::CreateView(TExprBase node, TExprContext& ctx) const {
    const auto& create = node.Cast<TYtCreateView>();
    const TYtTableInfo tableInfo(create.Table());
    const auto& descr = State_->TablesData->GetTable(create.DataSink().Cluster().StringValue(), tableInfo.Name, tableInfo.CommitEpoch);

    const auto& settings = Build<TCoNameValueTupleList>(ctx, create.Pos())
        .Add()
            .Name().Value("view", TNodeFlags::Default).Build()
            .Value(create.Original())
            .Build()
        .Done();

    const TYtOutTableInfo outTable(descr.RowType->Cast<TStructExprType>(), State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE, {}/*, descr.Meta->SqlView, settings*/);

    return Build<TYtPublish>(ctx, create.Pos())
        .World(create.World())
        .DataSink(create.DataSink())
        .Input()
            .Add<TYtOutput>()
                .Operation<TYtTouch>()
                    .World(create.World())
                    .DataSink(create.DataSink())
                    .Output()
                        .Add(outTable.ToExprNode(ctx, create.Table().Pos()))
                        .Build()
                    .Build()
                .OutIndex().Value(0U).Build()
                .Build()
            .Build()
        .Publish(create.Table())
        .Settings(create.Settings())
        .Done();
}

}  // namespace NYql

