#include "yt_graph.h"

#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/roren/yt/jobs.h>
#include <yt/cpp/roren/yt/proto/config.pb.h>
#include <yt/cpp/roren/yt/yt.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/writer.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NPrivate;

using TYtPipelineConfigCRef = std::reference_wrapper<const TYtPipelineConfig>;

////////////////////////////////////////////////////////////////////////////////

class TYtGraph::TTableNode
{
public:
    TTableNode(NYT::TRichYPath path, std::optional<TTableSchema> schema, TRowVtable rowVtable)
        : Path_(std::move(path))
        , Schema_(std::move(schema))
        , RowVtable_(std::move(rowVtable))
    { }

private:
    NYT::TRichYPath Path_;
    std::optional<TTableSchema> Schema_;
    TRowVtable RowVtable_;
    bool IsRemoved_ = false;

    std::set<TOperationNodeId> SourceFor_;
    std::optional<TOperationNodeId> SinkOf_;

    friend class TYtGraph;
};

class TYtGraph::TOperationNode
{
public:
    virtual ~TOperationNode() = default;

    virtual std::vector<TTableNodeId> GetSources() const = 0;
    virtual std::vector<TTableNodeId> GetSinks() const = 0;
    virtual void OptimizeLocally() = 0;
    virtual IRawTransformPtr GetRawTransform() const = 0;

    virtual NYT::IOperationPtr Start(const IClientBasePtr& client) const = 0;

protected:
    TOperationNodeId Id_;
    bool IsRemoved_ = false;

private:
    friend class TYtGraph;
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraph::TMapOperationNode
    : public TYtGraph::TOperationNode
{
public:
    TMapOperationNode(
        const TYtPipelineConfig& config,
        TYtGraph* graph,
        TTableNodeId input,
        std::vector<TTableNodeId> outputs,
        IRawTransformPtr rawTransform)
        : Config_(config)
        , Graph_(graph)
        , Input_(input)
        , Outputs_(std::move(outputs))
        , RawTransform_(std::move(rawTransform))
    {
        Init();
    }

    NYT::IOperationPtr Start(const IClientBasePtr& client) const override
    {
        auto spec = TRawMapOperationSpec()
            .AddInput(GetTableNode(Input_).Path_)
            .Format(NYT::TFormat::YsonBinary())
            .Pool(Config_.get().GetPool());

        for (auto outputId : Outputs_) {
            const auto& tableNode = GetTableNode(outputId);
            auto path = tableNode.Path_;
            if (tableNode.Schema_) {
                path.Schema(*tableNode.Schema_);
            }
            spec.AddOutput(path);
        }

        const auto& resourceFileList = TFnAttributesOps::GetResourceFileList(RawParDo_->GetFnAttributes());

        for (const auto& resourceFile : resourceFileList) {
            spec.MapperSpec_.AddLocalFile(resourceFile);
        }

        auto mapper = CreateParDoMap(RawParDo_, JobInput_, JobOutputs_);

        return client->RawMap(
            spec,
            mapper,
            TOperationOptions()
                .Wait(false)
        );
    }

    std::vector<TTableNodeId> GetSources() const override
    {
        return {Input_};
    }

    std::vector<TTableNodeId> GetSinks() const override
    {
        return Outputs_;
    }

    IRawTransformPtr GetRawTransform() const override
    {
        return RawTransform_;
    }

    void OptimizeLocally() override
    {
        switch (RawTransform_->GetType()) {
            case ERawTransformType::ParDo:
                OptimizeInput();
                OptimizeOutputs();
                return;
            case ERawTransformType::Read:
                return;
            case ERawTransformType::Write:
                OptimizeInput();
                return;
            default:
                Y_ABORT();
        }
    }

    void OptimizeInput()
    {
        auto& inputTable = Graph_->TableNodes_[Input_];
        Y_ABORT_UNLESS(inputTable.SinkOf_);
        auto inputOperationId = *inputTable.SinkOf_;
        const auto& inputOperation = Graph_->OperationNodes_[inputOperationId];

        auto* map = dynamic_cast<TMapOperationNode*>(inputOperation.get());
        if (!map || map->RawTransform_->GetType() != ERawTransformType::Read) {
            return;
        }

        const auto* rawYtInput = dynamic_cast<const IRawYtRead*>(map->RawTransform_->AsRawRead().Get());
        Y_ABORT_UNLESS(rawYtInput);
        JobInput_ = rawYtInput->CreateJobInput();

        auto newInputTableId = map->Input_;
        auto& newInputTable = Graph_->TableNodes_[newInputTableId];

        Y_ABORT_UNLESS(inputTable.SourceFor_.erase(Id_) == 1);
        if (inputTable.SourceFor_.empty()) {
            Graph_->RemoveTable(Input_);
            Graph_->RemoveOperation(inputOperationId);
            newInputTable.SourceFor_.erase(inputOperationId);
        }

        Input_ = newInputTableId;
        newInputTable.SourceFor_.insert(Id_);
    }

    void OptimizeOutputs()
    {
        std::vector<IYtJobOutputPtr> newJobOutputs;
        std::vector<TTableNodeId> newOutputs;

        auto optimizeOutput = [&](TOperationNodeId outputOperationId) -> std::optional<IYtJobOutputPtr> {
            auto* map = dynamic_cast<TMapOperationNode*>(Graph_->OperationNodes_[outputOperationId].get());
            if (!map || map->RawTransform_->GetType() != ERawTransformType::Write) {
                return std::nullopt;
            }

            Y_ABORT_UNLESS(map->Outputs_.size() == 1);
            auto newOutputTableId = map->Outputs_[0];
            auto newOutputIndex = std::ssize(newOutputs);
            newOutputs.push_back(newOutputTableId);

            auto& newOutputTable = Graph_->TableNodes_[newOutputTableId];
            newOutputTable.SinkOf_ = Id_;

            Graph_->RemoveOperation(outputOperationId);

            const auto* rawYtOutput = dynamic_cast<const IRawYtWrite*>(map->RawTransform_->AsRawWrite().Get());
            Y_ABORT_UNLESS(rawYtOutput);
            return rawYtOutput->CreateJobOutput(newOutputIndex);
        };

        for (int outputIndex = 0; outputIndex < std::ssize(Outputs_); ++outputIndex) {
            auto outputTableId = Outputs_[outputIndex];
            const auto& outputTable = Graph_->TableNodes_[outputTableId];

            std::vector<IYtJobOutputPtr> unitedJobOutputs;
            auto needOldOutput = false;
            for (auto outputOperationId : outputTable.SourceFor_) {
                auto maybeJobOutput = optimizeOutput(outputOperationId);
                if (maybeJobOutput) {
                    unitedJobOutputs.push_back(*maybeJobOutput);
                } else {
                    needOldOutput = true;
                }
            }
            if (needOldOutput) {
                int newOutputIndex = std::ssize(newOutputs);
                newOutputs.push_back(outputTableId);
                auto newJobOutput = JobOutputs_[outputIndex]->Clone();
                newJobOutput->SetSinkIndices({newOutputIndex});
                unitedJobOutputs.push_back(std::move(newJobOutput));
            } else {
                Graph_->RemoveTable(outputTableId);
            }
            newJobOutputs.push_back(CreateTeeJobOutput(std::move(unitedJobOutputs)));
        }

        JobOutputs_ = std::move(newJobOutputs);
        Outputs_ = std::move(newOutputs);
    }

private:
    const TTableNode& GetTableNode(TTableNodeId id) const
    {
        return Graph_->TableNodes_[id];
    }

    void Init()
    {
        switch (RawTransform_->GetType()) {
            case ERawTransformType::Read: {
                const auto* rawYtInput = dynamic_cast<const IRawYtRead*>(RawTransform_->AsRawRead().Get());
                Y_ABORT_UNLESS(rawYtInput);
                JobInput_ = rawYtInput->CreateJobInput();
                auto sinkRowVtable = GetTableNode(Outputs_[0]).RowVtable_;
                JobOutputs_ = {CreateEncodingJobOutput(sinkRowVtable, 0)};
                RawParDo_ = MakeRawIdComputation(sinkRowVtable);
                break;
            }
            case ERawTransformType::Write: {
                const auto* rawYtOutput = dynamic_cast<const IRawYtWrite*>(RawTransform_->AsRawWrite().Get());
                Y_ABORT_UNLESS(rawYtOutput);
                auto sourceRowVtable = GetTableNode(Input_).RowVtable_;
                JobInput_ = CreateDecodingJobInput(sourceRowVtable);
                JobOutputs_ = {rawYtOutput->CreateJobOutput()};
                RawParDo_ = MakeRawIdComputation(sourceRowVtable);
                break;
            }
            case ERawTransformType::ParDo: {
                JobInput_ = CreateDecodingJobInput(GetTableNode(Input_).RowVtable_);
                for (int sinkIndex = 0; auto outputId : Outputs_) {
                    auto sinkRowVtable = GetTableNode(outputId).RowVtable_;
                    JobOutputs_.emplace_back(CreateEncodingJobOutput(sinkRowVtable, sinkIndex));
                    sinkIndex++;
                }
                RawParDo_ = RawTransform_->AsRawParDo();
                break;
            }
            default:
                Y_ABORT();
        }
    }

private:
    TYtPipelineConfigCRef Config_;
    TYtGraph* Graph_;
    TTableNodeId Input_;
    std::vector<TTableNodeId> Outputs_;
    IRawTransformPtr RawTransform_;

    IYtJobInputPtr JobInput_;
    std::vector<IYtJobOutputPtr> JobOutputs_;
    IRawParDoPtr RawParDo_;

    friend class TYtGraph;
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraph::TMapReduceOperationNode
    : public TYtGraph::TOperationNode
{
public:
    TMapReduceOperationNode(
        const TYtPipelineConfig& config,
        TYtGraph* graph,
        std::vector<TTableNodeId> inputs,
        std::vector<TTableNodeId> mapOutputs,
        std::vector<TTableNodeId> outputs,
        IRawTransformPtr rawTransform,
        TYtStateVtable stateVtable
        )
        : Config_(config)
        , Graph_(graph)
        , Inputs_(std::move(inputs))
        , MapOutputs_(std::move(mapOutputs))
        , Outputs_(std::move(outputs))
        , RawTransform_(std::move(rawTransform))
        , StateVtable_(std::move(stateVtable))
    {
        Init();
    }

    NYT::IOperationPtr Start(const IClientBasePtr& client) const override
    {
        auto spec = TRawMapReduceOperationSpec()
            .MapperFormat(TFormat::YsonBinary())
            .ReduceCombinerFormat(TFormat::YsonBinary())
            .ReducerFormat(TFormat::YsonBinary())
            .ReduceBy({"key"})
            .Pool(Config_.get().GetPool());

        for (auto inputId : Inputs_) {
            spec.AddInput(GetTableNode(inputId).Path_);
        }
        for (auto mapOutputId : MapOutputs_) {
            spec.AddMapOutput(GetTableNode(mapOutputId).Path_);
        }
        for (auto outputId : Outputs_) {
            spec.AddOutput(GetTableNode(outputId).Path_);
        }

        TNode specPatch;
        specPatch["reduce_job_io"]["control_attributes"]["enable_key_switch"] = true;
        specPatch["force_reduce_combiners"] = ForceReduceCombiners_;

        return client->RawMapReduce(
            spec,
            Mapper_,
            ReduceCombiner_,
            Reducer_,
            TOperationOptions()
                .Spec(specPatch)
                .Wait(false)
        );
    }

    std::vector<TTableNodeId> GetSources() const override
    {
        return Inputs_;
    }

    std::vector<TTableNodeId> GetSinks() const override
    {
        auto sinks = MapOutputs_;
        sinks.insert(sinks.end(), Outputs_.begin(), Outputs_.end());
        return sinks;
    }

    IRawTransformPtr GetRawTransform() const override
    {
        return RawTransform_;
    }

    void OptimizeLocally() override
    {
        OptimizeInput();
        OptimizeOutputs();
    }

private:
    const TTableNode& GetTableNode(TTableNodeId id) const
    {
        return Graph_->TableNodes_[id];
    }

    std::vector<TRowVtable> GetRowVtables(const std::vector<TTableNodeId>& nodeIds)
    {
        std::vector<TRowVtable> rowVtables;
        rowVtables.reserve(nodeIds.size());

        for (const auto& nodeId : nodeIds) {
            rowVtables.emplace_back(GetTableNode(nodeId).RowVtable_);
        }
        return rowVtables;
    }

    void OptimizeInput()
    {
        if (Inputs_.size() > 1) {
            return;
        }

        auto inputTableId = Inputs_[0];
        auto& inputTable = Graph_->TableNodes_[inputTableId];
        if (inputTable.SourceFor_.size() != 1) {
            // It seems to be a rare case.
            return;
        }

        Y_ABORT_UNLESS(inputTable.SinkOf_);
        auto inputOperationId = *inputTable.SinkOf_;
        const auto& inputOperation = Graph_->OperationNodes_[inputOperationId];

        auto* map = dynamic_cast<TMapOperationNode*>(inputOperation.get());
        if (!map || map->RawTransform_->GetType() != ERawTransformType::ParDo) {
            return;
        }

        auto intermediateOutputIterator = std::find(map->Outputs_.begin(), map->Outputs_.end(), inputTableId);
        Y_ABORT_UNLESS(intermediateOutputIterator != map->Outputs_.end());
        auto intermediateOutputIndex = intermediateOutputIterator - map->Outputs_.begin();

        std::vector<IYtJobOutputPtr> mapperJobOutputs;
        mapperJobOutputs.reserve(map->Outputs_.size());

        Y_ABORT_UNLESS(MapOutputs_.empty());
        MapOutputs_.reserve(map->Outputs_.size() - 1);

        for (int i = 0; i < std::ssize(map->Outputs_); ++i) {
            if (i == intermediateOutputIndex) {
                mapperJobOutputs.push_back(CreateKvJobNodeOutput(/*sinkIndex*/ 0, {inputTable.RowVtable_}));
            } else {
                MapOutputs_.push_back(map->Outputs_[i]);
                auto newJobOutput = map->JobOutputs_[i]->Clone();
                auto sinkIndices = newJobOutput->GetSinkIndices();
                for (auto& sinkIndex : sinkIndices) {
                    ++sinkIndex;
                }
                newJobOutput->SetSinkIndices(sinkIndices);
                mapperJobOutputs.push_back(std::move(newJobOutput));
            }
        }

        Mapper_ = CreateParDoMap(map->RawParDo_, map->JobInput_, mapperJobOutputs);

        auto newInputTableId = map->Input_;

        Inputs_ = {newInputTableId};

        auto& newInputTable = Graph_->TableNodes_[newInputTableId];
        newInputTable.SourceFor_.erase(map->Id_);
        newInputTable.SourceFor_.insert(Id_);
        inputTable.SourceFor_.erase(Id_);

        Graph_->RemoveTable(inputTableId);
        Graph_->RemoveOperation(map->Id_);
    }

    void OptimizeOutputs()
    {
        if (RawTransform_->GetType() != ERawTransformType::StatefulParDo) {
            Y_ABORT_UNLESS(Outputs_.size() == 1);
        } else {
            return;  // TODO: optimize multiple outputs
        }
        auto& outputTable = Graph_->TableNodes_[Outputs_[0]];
        std::vector<TOperationNodeId> newOutputs;
        std::vector<IYtJobOutputPtr> newJobOutputs;

        int sinkIndex = 0;
        auto nextSinkIndex = [&] () {
            return sinkIndex++;
        };

        auto addOutputOperation = [&] (TMapOperationNode* map) {
            std::vector<IYtJobOutputPtr> jobOutputs;
            jobOutputs.reserve(map->JobOutputs_.size());
            for (const auto& originalJobOutput : map->JobOutputs_) {
                auto jobOutput = originalJobOutput->Clone();
                std::vector<int> sinkIndices;
                sinkIndices.reserve(jobOutput->GetSinkCount());
                for (int i = 0; i < jobOutput->GetSinkCount(); ++i) {
                    sinkIndices.push_back(nextSinkIndex());
                }
                jobOutput->SetSinkIndices(sinkIndices);
                jobOutputs.push_back(std::move(jobOutput));
            }

            newJobOutputs.push_back(CreateParDoJobOutput(map->RawParDo_, std::move(jobOutputs)));
            newOutputs.insert(newOutputs.end(), map->Outputs_.begin(), map->Outputs_.end());

            Graph_->RemoveOperation(map->Id_);
        };

        bool needOldOutput = false;
        for (auto outputOperationId : outputTable.SourceFor_) {
            const auto& outputOperation = Graph_->OperationNodes_[outputOperationId];

            auto* map = dynamic_cast<TMapOperationNode*>(outputOperation.get());
            if (!map) {
                needOldOutput = true;
                continue;
            }

            addOutputOperation(map);
        }

        auto outputVtables = GetRowVtables(Outputs_);
        if (needOldOutput) {
            newOutputs.push_back(Outputs_[0]);
            newJobOutputs.push_back(CreateEncodingJobOutput(outputVtables[0], nextSinkIndex()));
        } else {
            Graph_->RemoveTable(Outputs_[0]);
        }

        Outputs_ = std::move(newOutputs);
        for (const auto outputTableId: Outputs_) {
            Graph_->TableNodes_[outputTableId].SinkOf_ = Id_;
        }
        auto jobOutput = newJobOutputs.size() == 1
            ? newJobOutputs[0]
            : CreateTeeJobOutput(std::move(newJobOutputs));

        auto intermediateVtables = GetRowVtables(Inputs_);
        Reducer_ = CreateReducer(intermediateVtables, outputVtables, {jobOutput});
    }

    void Init()
    {
        auto intermediateVtables = GetRowVtables(Inputs_);
        auto outputVtables = GetRowVtables(Outputs_);

        switch (RawTransform_->GetType()) {
            case ERawTransformType::GroupByKey: {
                Mapper_ = CreateSplitKvMap(GetTableNode(Inputs_[0]).RowVtable_);
                ReduceCombiner_ = nullptr;
                ForceReduceCombiners_ = false;
                break;
            }
            case ERawTransformType::StatefulParDo: {
                Mapper_ = CreateSplitStateKvMap(intermediateVtables, StateVtable_);
                ReduceCombiner_ = nullptr;
                ForceReduceCombiners_ = false;
                break;
            }
            case ERawTransformType::CoGroupByKey: {
                Mapper_ = CreateSplitKvMap(intermediateVtables);
                ReduceCombiner_ = nullptr;
                ForceReduceCombiners_ = false;
                break;
            }
            case ERawTransformType::CombinePerKey: {
                Y_ABORT_UNLESS(intermediateVtables.size() == 1);
                Mapper_ = CreateSplitKvMap(intermediateVtables.front());
                ReduceCombiner_ = CreateCombineCombiner(RawTransform_->AsRawCombine(), intermediateVtables.front());
                ForceReduceCombiners_ = true;
                break;
            }
            default:
                Y_ABORT();
        }
        std::vector<IYtJobOutputPtr> jobOutputs;
        for (size_t outputIndex = 0; outputIndex < outputVtables.size(); ++outputIndex) {
            const auto& outputRowVtable = outputVtables[outputIndex];
            jobOutputs.emplace_back(CreateEncodingJobOutput(outputRowVtable, outputIndex));
        }
        Reducer_ = CreateReducer(intermediateVtables, outputVtables, jobOutputs);
    }

    ::TIntrusivePtr<IRawJob> CreateReducer(const std::vector<TRowVtable>& intermediateVtables, const std::vector<TRowVtable>& outputVtables, const std::vector<IYtJobOutputPtr>& jobOutputs)
    {
        switch (RawTransform_->GetType()) {
            case ERawTransformType::GroupByKey: {
                Y_ABORT_UNLESS(intermediateVtables.size() == 1);
                TParDoTreeBuilder userLogic;
                auto nodeIdList = userLogic.AddParDo(
                    CreateGbkImpulseReadNodeParDo(RawTransform_->AsRawGroupByKey()),
                    TParDoTreeBuilder::RootNodeId
                );
                Y_ABORT_UNLESS(nodeIdList.size() == 1);
                nodeIdList = userLogic.AddParDo(
                    CreateOutputParDo(jobOutputs[0], outputVtables[0]),
                    nodeIdList[0]
                );
                Y_ABORT_UNLESS(nodeIdList.size() == 0);
                return CreateImpulseJob(userLogic.Build());
            }
            case ERawTransformType::CoGroupByKey:
                return CreateMultiJoinKvReduce(RawTransform_->AsRawCoGroupByKey(), intermediateVtables, jobOutputs[0]);
            case ERawTransformType::StatefulParDo: {
                return CreateStatefulKvReduce(RawTransform_->AsRawStatefulParDo(), intermediateVtables, jobOutputs, StateVtable_);
            }
            case ERawTransformType::CombinePerKey:
                return CreateCombineReducer(RawTransform_->AsRawCombine(), outputVtables[0], jobOutputs[0]);
            default:
                Y_ABORT();
        }
    }

private:
    TYtPipelineConfigCRef Config_;
    TYtGraph* Graph_;
    std::vector<TTableNodeId> Inputs_;
    std::vector<TTableNodeId> MapOutputs_;
    std::vector<TTableNodeId> Outputs_;
    IRawTransformPtr RawTransform_;
    TYtStateVtable StateVtable_;

    ::TIntrusivePtr<IRawJob> Mapper_;
    ::TIntrusivePtr<IRawJob> ReduceCombiner_;
    ::TIntrusivePtr<IRawJob> Reducer_;
    bool ForceReduceCombiners_;

    friend class TYtGraph;
};

////////////////////////////////////////////////////////////////////////////////

TYtGraph::TYtGraph(const TYtPipelineConfig& config)
    : Config_(config)
{
}

TYtGraph::~TYtGraph() = default;

void TYtGraph::Optimize()
{
    auto GetSubGraphName = [](const size_t i) {
        return TString("g") + ToString(i);
    };
    size_t i = 0;
    TString str;
    TStringOutput out(str);
    out << "digraph G {" << Endl;
    out << DumpDOTSubGraph(GetSubGraphName(i++)) << Endl;

    // First optimize maps.
    for (auto& operationNode : OperationNodes_) {
        if (!operationNode->IsRemoved_ && dynamic_cast<TYtGraph::TMapOperationNode*>(operationNode.get())) {
            operationNode->OptimizeLocally();
            out << DumpDOTSubGraph(GetSubGraphName(i++)) << Endl;
        }
    }

    // Then map_reduces.
    for (auto& operationNode : OperationNodes_) {
        if (!operationNode->IsRemoved_ && dynamic_cast<TYtGraph::TMapReduceOperationNode*>(operationNode.get())) {
            operationNode->OptimizeLocally();
            out << DumpDOTSubGraph(GetSubGraphName(i++)) << Endl;
        }
    }
    out << '}' << Endl;
    // Cout << str << Flush;
}

TYtGraph::TTableNodeId TYtGraph::AddTableNode(NYT::TRichYPath path, std::optional<TTableSchema> schema, TRowVtable rowVtable)
{
    TableNodes_.emplace_back(std::move(path), std::move(schema), std::move(rowVtable));
    return std::ssize(TableNodes_) - 1;
}

TYtGraph::TOperationNodeId TYtGraph::AddMapOperationNode(
    TTableNodeId input,
    std::vector<TTableNodeId> outputs,
    IRawTransformPtr rawTransform)
{
    return AddOperationNode(std::make_unique<TMapOperationNode>(
        Config_,
        this,
        input,
        std::move(outputs),
        std::move(rawTransform)
    ));
}

TYtGraph::TOperationNodeId TYtGraph::AddMapReduceOperationNode(
    std::vector<TTableNodeId> inputs,
    std::vector<TTableNodeId> mapOutputs,
    std::vector<TTableNodeId> outputs,
    IRawTransformPtr rawTransform,
    TYtStateVtable stateVtable)
{
    return AddOperationNode(std::make_unique<TMapReduceOperationNode>(
        Config_,
        this,
        std::move(inputs),
        std::move(mapOutputs),
        std::move(outputs),
        std::move(rawTransform),
        std::move(stateVtable)
    ));
}

TYtGraph::TOperationNodeId TYtGraph::AddOperationNode(std::unique_ptr<TOperationNode> operationNode)
{
    auto operationNodeId = std::ssize(OperationNodes_);
    operationNode->Id_ = operationNodeId;
    OperationNodes_.push_back(std::move(operationNode));

    for (auto tableNodeId : OperationNodes_.back()->GetSources()) {
        auto& tableNode = TableNodes_[tableNodeId];
        tableNode.SourceFor_.insert(operationNodeId);
    }

    for (auto tableNodeId : OperationNodes_.back()->GetSinks()) {
        auto& tableNode = TableNodes_[tableNodeId];
        tableNode.SinkOf_ = operationNodeId;
    }

    return operationNodeId;
}

void TYtGraph::RemoveTable(TTableNodeId tableId)
{
    TableNodes_[tableId].IsRemoved_ = true;
}

void TYtGraph::RemoveOperation(TOperationNodeId operationId)
{
    OperationNodes_[operationId]->IsRemoved_ = true;
}

////////////////////////////////////////////////////////////////////////////////

NYT::IOperationPtr TYtGraph::StartOperation(const IClientBasePtr& client, TOperationNodeId id, const TStartOperationContext& /*context*/) const
{
    Y_ABORT_UNLESS(0 <= id && id < std::ssize(OperationNodes_));
    return OperationNodes_[id]->Start(client);
}

std::vector<std::vector<TYtGraph::TOperationNodeId>> TYtGraph::GetOperationLevels() const
{
    // check all inputs for operation
    auto readyForExecute = [&](const auto& operation, const THashSet<TTableNodeId> readyTables) {
        for (TTableNodeId sourceId : operation->GetSources()) {
            if (!readyTables.contains(sourceId)) {
                return false;
            }
        }
        return true;
    };

    std::vector<std::vector<TOperationNodeId>> result;
    THashSet<TOperationNodeId> processed;
    THashSet<TOperationNodeId> candidates;
    THashSet<TTableNodeId> readyTables;

    // populate readyTables with initial inputs
    for (TTableNodeId tableId = 0; tableId < std::ssize(TableNodes_); ++tableId) {
        const auto& tableNode = TableNodes_[tableId];
        if (!tableNode.IsRemoved_ && !tableNode.SinkOf_) {
            readyTables.emplace(tableId);
        }
    }

    // populate candidates with operations depended on initial inputs
    for (const TTableNodeId tableId: readyTables) {
        const auto& tableNode = TableNodes_[tableId];
        for (const TOperationNodeId operationId : tableNode.SourceFor_) {
            if (!OperationNodes_[operationId]->IsRemoved_) {
               candidates.emplace(operationId);
            }
        }
    }

    while (!candidates.empty()) {
        // gather ready to run operations
        std::vector<TOperationNodeId> levelOperations;
        for (TOperationNodeId operationId : candidates) {
            const auto& operation = OperationNodes_[operationId];
            if (readyForExecute(operation, readyTables)) {
                levelOperations.emplace_back(operationId);
            }
        }

        // we must run at least one opertaion
        Y_ABORT_UNLESS(!levelOperations.empty());
        result.emplace_back(std::move(levelOperations));

        // prepare for next iteration
        for (const TOperationNodeId operationId : result.back()) {
            // mark selected operations as processed
            candidates.erase(operationId);
            processed.emplace(operationId);

            // populate readyTables and candidates
            const auto& operation = OperationNodes_[operationId];
            for (const TOperationNodeId tableId : operation->GetSinks()) {
                const auto& tableNode = TableNodes_[tableId];
                if (!tableNode.IsRemoved_) {
                    readyTables.emplace(tableId);
                    for (const TOperationNodeId candidateId : tableNode.SourceFor_) {
                        const auto& candidate = OperationNodes_[candidateId];
                        if (!candidate->IsRemoved_ && !processed.contains(candidateId)) {
                            candidates.emplace(candidateId);
                        }
                    }
                }
            }
        }
    }

    return result;
}

TString TYtGraph::DumpDOTSubGraph(const TString& name) const
{
    TString str;
    TStringOutput out(str);
    out << "subgraph " << name << "{" << Endl;
    out << "color=grey" << Endl;
    out << DumpDOT(name) << Endl;
    out << '}' << Endl;
    return str;
}

TString TYtGraph::DumpDOT(const TString& prefix) const
{
    auto GetTableId = [&prefix] (const size_t i) -> TString {
        return prefix + "_t" + ToString(i);
    };
    auto GetOperationId = [&prefix] (const size_t i) -> TString {
        return prefix + "_o" + ToString(i);
    };
    auto FormatLabel = [] (const TString& label, const bool cond=true) -> TString {
        if (cond) {
            return TString("label=\"") + label + "\" ";
        }
        return TString();
    };
    auto FormatColor = [] (const TString& color, const bool cond=true) -> TString {
        if (cond) {
            return TString("color=") + color + ' ';
        }
        return TString();
    };
    TString str;
    TStringOutput out(str);
    for (ssize_t i = 0; i < std::ssize(TableNodes_); ++i) {
        const auto& table = TableNodes_[i];
        out << GetTableId(i) << " [" << FormatLabel(table.Path_.Path_) << FormatColor("red", table.IsRemoved_) << ']' << Endl;
        out << GetTableId(i) << "->{";
        for (const auto& outId : table.SourceFor_) {
            out << GetOperationId(outId) << ' ';
        }
        out << "} [" << FormatColor("red") << ']' << Endl;
        if (table.SinkOf_) {
            out << GetOperationId(table.SinkOf_.value()) << "->" << GetTableId(i) << " [" << FormatColor("red") << ']' << Endl;
        }
    }
    for (const auto& op : OperationNodes_) {
        out << GetOperationId(op->Id_) << " [" << FormatLabel(ToString(op->GetRawTransform()->GetType())) << FormatColor("red", op->IsRemoved_) << ']' << Endl;
        out << '{';
        for (const auto& inId: op->GetSources()) {
            out << GetTableId(inId) << ' ';
        }
        out << "}->" << GetOperationId(op->Id_) << " [" << FormatColor("blue") << ']' << Endl;
        out << GetOperationId(op->Id_) << "->{";
        for (const auto& outId: op->GetSinks()) {
            out << GetTableId(outId) << ' ';
        }
        out << "} [" << FormatColor("blue") << ']' << Endl;
    }
    return str;
}

////////////////////////////////////////////////////////////////////////////////

class TBuildingVisitor
    : public IRawPipelineVisitor
{
public:
    TBuildingVisitor(const TYtPipelineConfig& config, TYtGraph* graph)
        : Config_(config)
        , Graph_(*graph)
    { }

    void OnTransform(TTransformNode* transform) override
    {
        auto rawTransform = transform->GetRawTransform();
        switch (rawTransform->GetType()) {
            case ERawTransformType::Read:
                if (const auto* rawYtInput = dynamic_cast<const IRawYtRead*>(&*rawTransform->AsRawRead())) {
                    auto inputNode = Graph_.AddTableNode(
                        rawYtInput->GetPath(),
                        /*schema*/ std::nullopt,
                        TRowVtable{});
                    auto outputNodes = AddTemporaryTableNodes(transform->GetSinkList());
                    Graph_.AddMapOperationNode(inputNode, outputNodes, transform->GetRawTransform());
                } else {
                    THROW_NOT_IMPLEMENTED_YET();
                }
                break;
            case ERawTransformType::Write:
                if (const auto* rawYtOutput = dynamic_cast<const IRawYtWrite*>(&*rawTransform->AsRawWrite())) {
                    auto inputNode = MapPCollectionToTableNode(transform->GetSource(0));
                    auto outputNode = Graph_.AddTableNode(
                        rawYtOutput->GetPath(),
                        rawYtOutput->GetSchema(),
                        TRowVtable{});
                    Graph_.AddMapOperationNode(inputNode, {outputNode}, transform->GetRawTransform());
                } else {
                    THROW_NOT_IMPLEMENTED_YET();
                }
                break;
            case ERawTransformType::GroupByKey:
            case ERawTransformType::CombinePerKey: {
                auto inputNode = MapPCollectionToTableNode(transform->GetSource(0));
                auto outputNodes = AddTemporaryTableNodes(transform->GetSinkList());
                Graph_.AddMapReduceOperationNode(
                    /*inputs*/ {inputNode},
                    /*mapOutputs*/ {},
                    /*outputs*/ outputNodes,
                    transform->GetRawTransform(),
                    {}
                );
                break;
            }
            case ERawTransformType::CoGroupByKey: {
                auto inputNodes = MapPCollectionsToTableNodes(transform->GetSourceList());
                auto outputNodes = AddTemporaryTableNodes(transform->GetSinkList());
                Graph_.AddMapReduceOperationNode(
                    /*inputs*/ inputNodes,
                    /*mapOutputs*/ {},
                    /*outputs*/ outputNodes,
                    transform->GetRawTransform(),
                    {}
                );
                break;
            }
            case ERawTransformType::ParDo: {
                auto inputNode = MapPCollectionToTableNode(transform->GetSource(0));
                auto outputNodes = AddTemporaryTableNodes(transform->GetSinkList());
                Graph_.AddMapOperationNode(inputNode, outputNodes, transform->GetRawTransform());
                break;
            }
            case ERawTransformType::StatefulParDo: {
                const TYtStateVtable* stateVtable = NPrivate::GetAttribute(*transform->GetPStateNode(), YtStateVtableTag);
                const TString* stateInPath = NPrivate::GetAttribute(*transform->GetPStateNode(), YtStateInPathTag);
                const TString* stateOutPath = NPrivate::GetAttribute(*transform->GetPStateNode(), YtStateOutPathTag);
                Y_ABORT_UNLESS(stateVtable);
                Y_ABORT_UNLESS(stateInPath);
                Y_ABORT_UNLESS(stateOutPath);

                auto stateInNode = Graph_.AddTableNode(
                    *stateInPath,
                    /*schema*/ std::nullopt,
                    stateVtable->StateTKVvtable);
                auto stateOutNode = Graph_.AddTableNode(
                    *stateOutPath,
                    /*schema*/ std::nullopt,
                    stateVtable->StateTKVvtable);

                auto inputNode = MapPCollectionToTableNode(transform->GetSource(0));
                auto outputNodes = AddTemporaryTableNodes(transform->GetSinkList());
                outputNodes.emplace(outputNodes.begin(), stateOutNode);
                Graph_.AddMapReduceOperationNode(
                    /*inputs*/ {stateInNode, inputNode},
                    /*mapOutputs*/ {},
                    /*outputs*/ outputNodes,
                    transform->GetRawTransform(),
                    *stateVtable
                );
                break;
            }
            default:
                Y_ABORT();
        }
    }

private:
    TYtGraph::TTableNodeId MapPCollectionToTableNode(const TPCollectionNodePtr& pCollection)
    {
        auto it = PCollectionToTableNodeId_.find(pCollection.Get());
        Y_ABORT_UNLESS(it != PCollectionToTableNodeId_.end());
        return it->second;
    }

    std::vector<TYtGraph::TTableNodeId> MapPCollectionsToTableNodes(const std::vector<TPCollectionNodePtr>& pCollections)
    {
        std::vector<TYtGraph::TTableNodeId> nodes;
        nodes.reserve(pCollections.size());
        std::transform(
            pCollections.begin(),
            pCollections.end(),
            std::back_inserter(nodes),
            [this](const TPCollectionNodePtr& collection) {
                return MapPCollectionToTableNode(collection);
            });
        return nodes;
    }

    std::vector<TYtGraph::TTableNodeId> AddTemporaryTableNodes(const std::vector<TPCollectionNodePtr>& pCollections)
    {
        std::vector<TYtGraph::TTableNodeId> ytTableNodes;
        ytTableNodes.reserve(pCollections.size());
        for (const auto& pCollection : pCollections) {
            auto path = Config_.get().GetWorkingDir() + Sprintf("/roren-table-%d", pCollection->GetId());
            auto ytTableNode = Graph_.AddTableNode(
                path,
                /*schema*/ std::nullopt,
                pCollection->GetRowVtable()
            );
            ytTableNodes.push_back(ytTableNode);
            auto [it, inserted] = PCollectionToTableNodeId_.emplace(pCollection.Get(), ytTableNode);
            Y_ABORT_UNLESS(inserted);
        }
        return ytTableNodes;
    }

private:
    TYtPipelineConfigCRef Config_;
    TYtGraph& Graph_;
    THashMap<const TPCollectionNode*, TYtGraph::TTableNodeId> PCollectionToTableNodeId_;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IYtGraph> BuildYtGraph(const TPipeline& pipeline, const TYtPipelineConfig& config)
{
    auto graph = std::make_shared<TYtGraph>(config);
    TBuildingVisitor visitor(config, graph.get());
    TraverseInTopologicalOrder(GetRawPipeline(pipeline), &visitor);
    return graph;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
