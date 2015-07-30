#pragma once

#include "public.h"
#include "token.h"
#include "consumer.h"

#include <util/generic/noncopyable.h>
#include <util/stream/output.h>

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

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;

    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;

    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

    virtual void OnRaw(const TStringBuf& yson, EYsonType type = YT_NODE) override;

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

} // namespace NYT
