#include "list_fragment_parser.h"
#include "helpers.h"
#include "stream.h"

#include <yt/core/yson/lexer_detail.h>

#include <yt/core/ytree/node.h>

#include <yt/core/misc/finally.h>

#include <numeric>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TListFragmentParser::TImpl
{
public:
    explicit TImpl(IInputStream* stream)
        : InputStream_(TStreamReader(stream))
        , Consumer_(BIND(
            [&] () {
                CheckItem();
            }))
        , Parser_(&Consumer_, NYson::EYsonType::ListFragment)
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
                Parser_.Read(InputStream_.Current(), InputStream_.End());
                InputStream_.Advance(length);
            } else {
                IsStreamFinished_ = true;
                Parser_.Finish();
            }
        }

        if (Rows_.empty()) {
            CheckItem();
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
    bool IsStreamFinished_ = false;
    std::queue<TSharedRef> Rows_;
};

////////////////////////////////////////////////////////////////////////////////

TListFragmentParser::TListFragmentParser()
{ }

TListFragmentParser::TListFragmentParser(IInputStream* stream)
    : Impl_(new TImpl(stream))
{ }

TSharedRef TListFragmentParser::NextItem()
{
    return Impl_->NextItem();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
