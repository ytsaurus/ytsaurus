#pragma once

#include "public.h"
// #include "helpers.h"

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/tree_builder.h>

#include <yt/core/json/config.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/utf8_decoder.h>

#include <queue>

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJsonCallbacksNodeType,
    (List)
    (Map)
);

class TJsonCallbacks
{
public:
    virtual void OnStringScalar(TStringBuf value) = 0;
    virtual void OnInt64Scalar(i64 value) = 0;
    virtual void OnUint64Scalar(ui64 value) = 0;
    virtual void OnDoubleScalar(double value) = 0;
    virtual void OnBooleanScalar(bool value) = 0;
    virtual void OnEntity() = 0;
    virtual void OnBeginList() = 0;
    virtual void OnEndList() = 0;
    virtual void OnBeginMap() = 0;
    virtual void OnKeyedItem(TStringBuf key) = 0;
    virtual void OnEndMap() = 0;

    virtual ~TJsonCallbacks()
    { };
};

////////////////////////////////////////////////////////////////////////////////

class TJsonCallbacksBuildingNodesImpl
    : public TJsonCallbacks
{
public:
    TJsonCallbacksBuildingNodesImpl(
        NYson::IYsonConsumer* consumer,
        NYson::EYsonType ysonType,
        const TUtf8Transcoder& utf8Transcoder,
        i64 memoryLimit,
        NJson::EJsonAttributesMode attributesMode);

    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf key) override;
    virtual void OnEndMap() override;

private:
    // Memory accounted approximately
    void AccountMemory(i64 memory);
    void OnItemStarted();
    void OnItemFinished();

    void ConsumeNode(NYTree::INodePtr node);
    void ConsumeNode(NYTree::IMapNodePtr map);
    void ConsumeNode(NYTree::IListNodePtr list);
    void ConsumeMapFragment(NYTree::IMapNodePtr map);

    NYson::IYsonConsumer* Consumer_;
    NYson::EYsonType YsonType_;
    TUtf8Transcoder Utf8Transcoder_;
    i64 ConsumedMemory_ = 0;
    const i64 MemoryLimit_;
    const NJson::EJsonAttributesMode AttributesMode_;

    SmallVector<EJsonCallbacksNodeType, 4> Stack_;

    const std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

class TJsonCallbacksForwardingImpl
    : public TJsonCallbacks
{
public:
    TJsonCallbacksForwardingImpl(
        NYson::IYsonConsumer* consumer,
        NYson::EYsonType ysonType,
        const TUtf8Transcoder& utf8Transcoder);

    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf key) override;
    virtual void OnEndMap() override;

private:
    void OnItemStarted();
    void OnItemFinished();

    NYson::IYsonConsumer* Consumer_;
    NYson::EYsonType YsonType_;
    TUtf8Transcoder Utf8Transcoder_;

    SmallVector<EJsonCallbacksNodeType, 4> Stack_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
