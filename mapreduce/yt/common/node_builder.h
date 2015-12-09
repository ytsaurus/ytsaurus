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

    virtual void OnStringScalar(const TStringBuf&) override;
    virtual void OnInt64Scalar(i64) override;
    virtual void OnUint64Scalar(ui64) override;
    virtual void OnDoubleScalar(double) override;
    virtual void OnBooleanScalar(bool) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf&) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

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

    virtual bool OnNull() override;
    virtual bool OnBoolean(bool val) override;
    virtual bool OnInteger(long long val) override;
    virtual bool OnUInteger(unsigned long long val) override;
    virtual bool OnString(const TStringBuf &val) override;
    virtual bool OnDouble(double val) override;
    virtual bool OnOpenArray() override;
    virtual bool OnCloseArray() override;
    virtual bool OnOpenMap() override;
    virtual bool OnCloseMap() override;
    virtual bool OnMapKey(const TStringBuf &val) override;

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
