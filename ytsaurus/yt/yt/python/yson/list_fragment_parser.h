#pragma once

#include <yt/yt/python/common/stream.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/lexer_detail.h>
#include <yt/yt/core/yson/parser.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <queue>
#include <stack>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TListFragmentConsumer
    : public NYson::IYsonConsumer
{
public:
    explicit TListFragmentConsumer(TCallback<void()> checkItemCallback)
        : CheckItemCallback_(checkItemCallback)
    { }

    void OnListItem() override
    {
        if (Balance_ == -1) {
            Balance_ = 0;
        } else {
            CheckItem();
        }
    }

    void OnKeyedItem(TStringBuf /*key*/) override
    { }

    void OnBeginAttributes() override
    {
        Balance_++;
    }

    void OnEndAttributes() override
    {
        Balance_--;
    }

    void OnRaw(TStringBuf /*yson*/, NYson::EYsonType /*type*/) override
    {
        YT_ABORT();
    }

    void OnStringScalar(TStringBuf /*value*/) override
    { }

    void OnInt64Scalar(i64 /*value*/) override
    { }

    void OnUint64Scalar(ui64 /*value*/) override
    { }

    void OnDoubleScalar(double /*value*/) override
    { }

    void OnBooleanScalar(bool /*value*/) override
    { }

    void OnEntity() override
    { }

    void OnBeginList() override
    {
        Balance_++;
    }

    void OnEndList() override
    {
        Balance_--;
    }

    void OnBeginMap() override
    {
        Balance_++;
    }

    void OnEndMap() override
    {
        Balance_--;
    }

private:
    void CheckItem()
    {
        if (Balance_ == 0) {
            CheckItemCallback_();
        }
    }

    TCallback<void()> CheckItemCallback_;

    int Balance_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

class TListFragmentParser
{
public:
    TListFragmentParser();
    TListFragmentParser(IInputStream* stream, int nestingLevelLimit);

    TSharedRef NextItem();

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
