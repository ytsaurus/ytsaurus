#pragma once

#include "common.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonWriter
    : public IYsonConsumer
    , private TNonCopyable
{
public:
    TYsonWriter(TOutputStream* stream, bool isBinary = false);

private:
    TOutputStream* Stream;
    bool IsFirstItem;
    bool IsEmptyEntity;
    int Indent;

    bool IsBinary;

    static const int IndentSize = 4;

    void WriteIndent();

    void SetEmptyEntity();
    void ResetEmptyEntity();
    void FlushEmptyEntity();

    void BeginCollection(char openBracket);
    void CollectionItem(char separator);
    void EndCollection(char closeBracket);

    virtual void OnStringScalar(const Stroka& value);
    virtual void OnInt64Scalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntityScalar();

    virtual void OnBeginList();
    virtual void OnListItem(int index);
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnMapItem(const Stroka& name);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const Stroka& name);
    virtual void OnEndAttributes();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

