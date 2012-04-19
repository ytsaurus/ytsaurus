#pragma once

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

    //! Inserts a portion of raw YSON into the stream.
    void OnRaw(const TStringBuf& yson, EYsonType type = EYsonType::Node);

protected:
    TOutputStream* Stream;
    bool IsFirstItem;
    bool IsEmptyEntity;
    int Indent;
    EYsonFormat Format;
    bool FormatRaw;

    static const int IndentSize = 4;

    void WriteIndent();
    void WriteStringScalar(const TStringBuf& value);
    void WriteMapItem(const TStringBuf& name);

    virtual void BeginCollection(char openBracket);
    virtual void CollectionItem(char separator);
    virtual void EndCollection(char closeBracket);

};

////////////////////////////////////////////////////////////////////////////////

class TYsonFragmentWriter
    : public TYsonWriter
{
public:
    //! Initializes an instance.
    /*!
     *  \param stream A stream for outputting the YSON data.
     *  \param format A format used for encoding the data.
     */
    TYsonFragmentWriter(
        TOutputStream* stream,
        EYsonFormat format = EYsonFormat::Binary,
        bool formatRaw = false);

    virtual void BeginCollection(char openBracket);
    virtual void CollectionItem(char separator);
    virtual void EndCollection(char closeBracket);
private:
    int NestedCount;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

