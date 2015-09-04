#include "helpers.h"

#include "format.h"

#include <core/misc/error.h>

#include <core/yson/format.h>
#include <core/ytree/yson_string.h>
#include <core/yson/token.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

const i64 ContextBufferSize = (i64) 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TFormatsConsumerBase::TFormatsConsumerBase()
    : Parser(this)
{ }
    

void TFormatsConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    Parser.Parse(yson, type);
}

////////////////////////////////////////////////////////////////////////////////

TContextSavingMixin::TContextSavingMixin(
    bool enableContextSaving,
    std::unique_ptr<TOutputStream> output)
    : EnableContextSaving_(enableContextSaving)
    , Output_(std::move(output))
{
    CurrentBuffer_.Reserve(ContextBufferSize);

    if (EnableContextSaving_) {
        PreviousBuffer_.Reserve(ContextBufferSize);
    }
}

TOutputStream* TContextSavingMixin::GetOutputStream()
{
    return &CurrentBuffer_;
}

TBlob TContextSavingMixin::GetContext() const
{
    TBlob result;
    result.Append(TRef::FromBlob(PreviousBuffer_.Blob()));
    result.Append(TRef::FromBlob(CurrentBuffer_.Blob()));
    return result;
}

void TContextSavingMixin::TryFlushBuffer()
{
    DoFlushBuffer(false);
}

void TContextSavingMixin::DoFlushBuffer(bool force)
{
    if (CurrentBuffer_.Size() == 0) {
        return;
    }

    if (!force && CurrentBuffer_.Size() < ContextBufferSize && EnableContextSaving_) {
        return;
    }

    const auto& buffer = CurrentBuffer_.Blob();
    Output_->Write(buffer.Begin(), buffer.Size());

    if (EnableContextSaving_) {
        std::swap(PreviousBuffer_, CurrentBuffer_);
    }
    CurrentBuffer_.Clear();
}

void TContextSavingMixin::Close()
{
    DoFlushBuffer(true);
    Output_->Finish();
}

////////////////////////////////////////////////////////////////////////////////

bool IsSpecialJsonKey(const TStringBuf& key)
{
    return key.size() > 0 && key[0] == '$';
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
