#pragma once

#include "public.h"

#include <ytlib/ytree/attributes.h>

namespace NYT {
namespace NYPath {

////////////////////////////////////////////////////////////////////////////////

//! YPath string plus attributes.
class TRichYPath
{
public:
    TRichYPath();
    TRichYPath(const TRichYPath& other);
    TRichYPath(TRichYPath&& other);
    TRichYPath(const char* path);
    TRichYPath(const TYPath& path);
    TRichYPath(const TYPath& path, const NYTree::IAttributeDictionary& attributes);
    
    TRichYPath& operator = (const TRichYPath& other);

    const TYPath& GetPath() const;
    void SetPath(const TYPath& path);

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes();

private:
    TYPath Path_;
    TAutoPtr<NYTree::IAttributeDictionary> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRichYPath& path);
void Serialize(const TRichYPath& richPath, NYTree::IYsonConsumer* consumer);
void Deserialize(TRichYPath& richPath, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
