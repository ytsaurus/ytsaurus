#pragma once

#include "common.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Creates a YSON data stream from a sequence of YSON events.
class TYsonWriter
    : public IYsonConsumer
    , private TNonCopyable
{
public:
    //! The data format.
    DECLARE_ENUM(EFormat,
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

    //! Initializes an instance.
    /*!
     *  \param stream A stream for outputting the YSON data.
     *  \param format A format used for encoding the data.
     */
    TYsonWriter(TOutputStream* stream, EFormat format);

    virtual void OnStringScalar(const Stroka& value, bool hasAttributes);
    virtual void OnInt64Scalar(i64 value, bool hasAttributes);
    virtual void OnDoubleScalar(double value, bool hasAttributes);
    virtual void OnEntity(bool hasAttributes);

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList(bool hasAttributes);

    virtual void OnBeginMap();
    virtual void OnMapItem(const Stroka& name);
    virtual void OnEndMap(bool hasAttributes);

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const Stroka& name);
    virtual void OnEndAttributes();

private:
    TOutputStream* Stream;
    bool IsFirstItem;
    bool IsEmptyEntity;
    int Indent;
    EFormat Format;

    static const int IndentSize = 4;

    void WriteIndent();
    void WriteStringScalar(const Stroka& value);
    void WriteMapItem(const Stroka& name);

    void BeginCollection(char openBracket);
    void CollectionItem(char separator);
    void EndCollection(char closeBracket);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

