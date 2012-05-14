#pragma once

#include "public.h"
#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! The data format.
DECLARE_ENUM(EYsonFormat,
    // Binary.
    // Most compact but not human-readable.
    (Binary)

    // Text.
    // Not so compact but human-readable.
    // Does not use indentation.
    // Uses escaping for non-text characters.
    (Text)

    // Text with indentation.
    // Extremely verbose but human-readable.
    // Uses escaping for non-text characters.
    (Pretty)
);

//! Creates a YSON data stream from a sequence of YSON events.
class TYsonWriter
    : public TYsonConsumerBase
    , private TNonCopyable
{
public:
    //! Initializes an instance.
    /*!
     *  \param stream A stream for outputting the YSON data.
     *  \param format A format used for encoding the data.
     */
    TYsonWriter(
        TOutputStream* stream,
        EYsonFormat format = EYsonFormat::Binary,
        EYsonType type = EYsonType::Node,
        bool formatRaw = false);

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnIntegerScalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntity();

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnKeyedItem(const TStringBuf& key);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnEndAttributes();

    virtual void OnRaw(const TStringBuf& yson, EYsonType type = EYsonType::Node);

protected:
    TOutputStream* Stream;
    EYsonFormat Format;
    EYsonType Type;
    bool FormatRaw;
    
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

} // namespace NYTree
} // namespace NYT

