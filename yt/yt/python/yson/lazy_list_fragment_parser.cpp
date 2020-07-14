#include "lazy_list_fragment_parser.h"
#include "lazy_yson_consumer.h"

namespace NYT::NPython {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TLazyListFragmentParser::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(
        IInputStream* stream,
        const std::optional<TString>& encoding,
        bool alwaysCreateAttributes,
        TPythonStringCache* keyCacher)
        : InputStream_(TStreamReader(stream))
        , Consumer_(
            BIND(&TImpl::ExtractPrefix, Unretained(this)),
            keyCacher,
            encoding,
            alwaysCreateAttributes)
        , Parser_(&Consumer_, NYson::EYsonType::ListFragment)
    { }

    PyObject* NextItem()
    {
        while (!Consumer_.HasObject() && !IsStreamFinished_) {
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

        if (!Consumer_.HasObject()) {
            return nullptr;
        }

        auto nextItem = Consumer_.ExtractObject();
        nextItem.increment_reference_count();
        return *nextItem;
    }

private:
    TSharedRef ExtractPrefix()
    {
        auto current = Parser_.GetCurrentPositionInBlock();
        if (!current) {
            current = InputStream_.End();
        }
        auto row = InputStream_.ExtractPrefix(current);
        return row;
    }

    TStreamReader InputStream_;
    TLazyYsonConsumer Consumer_;
    NYson::TYsonParser Parser_;
    bool IsStreamFinished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TLazyListFragmentParser::TLazyListFragmentParser() = default;

TLazyListFragmentParser::TLazyListFragmentParser(
    IInputStream* stream,
    const std::optional<TString>& encoding,
    bool alwaysCreateAttributes,
    TPythonStringCache* keyCacher)
    : Impl_(New<TImpl>(stream, encoding, alwaysCreateAttributes, keyCacher))
{ }

TLazyListFragmentParser::~TLazyListFragmentParser() = default;

PyObject* TLazyListFragmentParser::NextItem()
{
    return Impl_->NextItem();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
