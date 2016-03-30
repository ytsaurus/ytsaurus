#pragma once

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/yson/consumer.h>

#include <library/json/json_reader.h>

#include <util/generic/stack.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeBuilder
    : public TYsonConsumerBase
{
public:
    TNodeBuilder(TNode* node);

    void OnStringScalar(const TStringBuf&) override;
    void OnInt64Scalar(i64) override;
    void OnUint64Scalar(ui64) override;
    void OnDoubleScalar(double) override;
    void OnBooleanScalar(bool) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(const TStringBuf&) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;

private:
    ystack<TNode*> Stack_;

private:
    inline void AddNode(TNode node, bool pop);
};

class TYson2JsonCallbacksAdapter
    : public NJson::TJsonCallbacks
{
public:
    TYson2JsonCallbacksAdapter(TYsonConsumerBase* impl);

    bool OnNull() override;
    bool OnBoolean(bool val) override;
    bool OnInteger(long long val) override;
    bool OnUInteger(unsigned long long val) override;
    bool OnString(const TStringBuf &val) override;
    bool OnDouble(double val) override;
    bool OnOpenArray() override;
    bool OnCloseArray() override;
    bool OnOpenMap() override;
    bool OnCloseMap() override;
    bool OnMapKey(const TStringBuf &val) override;

private:
    void WrapIfListItem();

private:
    TYsonConsumerBase* Impl;
    // Stores current context stack
    // If true - we are in a list
    // If false - we are in a map
    ystack<bool> ContextStack;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
