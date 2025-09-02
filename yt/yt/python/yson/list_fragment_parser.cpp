#include "list_fragment_parser.h"

#include <yt/yt/python/common/helpers.h>

#include <yt/yt/core/ytree/node.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TListFragmentParser::TImpl
{
public:
    explicit TImpl(IInputStream* stream, int nestingLevelLimit)
        : InputStream_(TStreamReader(stream))
        , Consumer_(BIND(
            [&] {
                CheckItem();
            }))
        , Parser_(
            &Consumer_,
            NYson::EYsonType::ListFragment,
            NYson::TYsonParserConfig{
                .NestingLevelLimit = nestingLevelLimit,
            })
    { }

    TSharedRef NextItem()
    {
        while (Rows_.empty() && !IsStreamFinished_) {
            auto length = InputStream_.End() - InputStream_.Current();
            if (length == 0 && !InputStream_.IsFinished()) {
                InputStream_.RefreshBlock();
                continue;
            }

            if (length != 0) {
                IsEmpty_ = false;
                Parser_.Read(InputStream_.Current(), InputStream_.End());
                InputStream_.Advance(length);
            } else {
                IsStreamFinished_ = true;
                CheckItem();
                Parser_.Finish();
            }
        }

        if (Rows_.empty()) {
            return TSharedRef();
        }

        auto row = Rows_.front();
        Rows_.pop();
        return row;
    }

private:
    void CheckItem()
    {
        if (IsEmpty_) {
            return;
        }

        auto current = Parser_.GetCurrentPositionInBlock();
        if (!current) {
            current = InputStream_.End();
        }

        auto row = InputStream_.ExtractPrefix(current);
        if (row.Size() == 0) {
            return;
        }
        Rows_.push(row);
    }

    TStreamReader InputStream_;
    TListFragmentConsumer Consumer_;
    NYson::TYsonParser Parser_;
    bool IsEmpty_ = true;
    bool IsStreamFinished_ = false;
    std::queue<TSharedRef> Rows_;
};

////////////////////////////////////////////////////////////////////////////////

TListFragmentParser::TListFragmentParser()
{ }

TListFragmentParser::TListFragmentParser(IInputStream* stream, int nestingLevelLimit)
    : Impl_(new TImpl(stream, nestingLevelLimit))
{ }

TSharedRef TListFragmentParser::NextItem()
{
    return Impl_->NextItem();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
