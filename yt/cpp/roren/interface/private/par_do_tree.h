#pragma once

#include "fwd.h"
#include "raw_transform.h"

#include <list>
#include <vector>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IParDoTree
    : public IRawParDo
{
public:
    virtual std::vector<TDynamicTypeTag> GetOriginalOutputTags() const = 0;
    virtual TString GetDebugDescription() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TParDoTreeBuilder
{
public:
    using TPCollectionNodeId = int;
    static constexpr TPCollectionNodeId RootNodeId = 0;

public:
    std::vector<TPCollectionNodeId> AddParDo(IRawParDoPtr parDo, TPCollectionNodeId input);
    TPCollectionNodeId AddParDoVerifySingleOutput(IRawParDoPtr parDo, TPCollectionNodeId input);
    void AddParDoVerifyNoOutput(IRawParDoPtr parDo, TPCollectionNodeId input);

    std::vector<TPCollectionNodeId> AddParDoChain(TPCollectionNodeId input, const std::vector<IRawParDoPtr>& parDoList);
    TPCollectionNodeId AddParDoChainVerifySingleOutput(TPCollectionNodeId input, const std::vector<IRawParDoPtr>& parDoList);
    void AddParDoChainVerifyNoOutput(TPCollectionNodeId input, const std::vector<IRawParDoPtr>& parDoList);

    THashMap<TPCollectionNodeId, TPCollectionNodeId> Fuse(const TParDoTreeBuilder& other, TPCollectionNodeId input);

    void MarkAsOutput(TPCollectionNodeId nodeId, const TDynamicTypeTag& typeTag = {});
    void MarkAsOutputs(const std::vector<TPCollectionNodeId>& nodeIds);
    IParDoTreePtr Build();

    bool Empty() const
    {
        return ParDoNodes_.empty();
    }

private:
    struct TParDoNode
    {
        IRawParDoPtr ParDo;
        TPCollectionNodeId Input;
        std::vector<TPCollectionNodeId> Outputs;

        Y_SAVELOAD_DEFINE(ParDo, Input, Outputs);
    };

    struct TPCollectionNode
    {
        int GlobalOutputIndex = InvalidOutputIndex;
        TRowVtable RowVtable;
    };

private:
    TPCollectionNodeId AddPCollectionNode(const TRowVtable& sourceParDoNode);
    void CheckNoHangingPCollectionNodes() const;
    void CheckPCollectionType(int pCollectionNodeId, TStringBuf expectedDescription, const TRowVtable& expectedRowVtable);

private:
    static constexpr int InvalidOutputIndex = -1;

    std::vector<TPCollectionNode> PCollectionNodes_ = {{}};
    std::vector<TParDoNode> ParDoNodes_;

    // Type tags of outputs marked with 'MarkAsOutput' / 'MarkAsOutputs'
    std::vector<TDynamicTypeTag> MarkedOutputTypeTags_;

    bool Built_ = false;

    class TParDoTree;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
