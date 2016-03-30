#pragma once

#include "public.h"
#include "token.h"
#include "consumer.h"

#include <util/generic/noncopyable.h>

class TOutputStream;
class TZeroCopyInput;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TYsonWriter
    : public TYsonConsumerBase
    , private TNonCopyable
{
public:
    TYsonWriter(
        TOutputStream* stream,
        EYsonFormat format = YF_BINARY,
        EYsonType type = YT_NODE,
        bool enableRaw = false);

    void OnStringScalar(const TStringBuf& value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(const TStringBuf& key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    void OnRaw(const TStringBuf& yson, EYsonType type = YT_NODE) override;

protected:
    TOutputStream* Stream;
    EYsonFormat Format;
    EYsonType Type;
    bool EnableRaw;

    int Depth;
    bool BeforeFirstItem;

    static const int IndentSize = 4;

    void WriteIndent();
    void WriteStringScalar(const TStringBuf& value);

    void BeginCollection(ETokenType beginToken);
    void CollectionItem(ETokenType separatorToken);
    void EndCollection(ETokenType endToken);

    bool IsTopLevelFragmentContext() const;
    void EndNode();
};

////////////////////////////////////////////////////////////////////////////////

void ReformatYsonStream(
    TZeroCopyInput* input,
    TOutputStream* output,
    EYsonFormat format = YF_BINARY,
    EYsonType type = YT_NODE);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
