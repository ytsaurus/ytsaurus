#pragma once

#include "public.h"
#include "attributes.h"

namespace NYT {
namespace NYTree {

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
    TRichYPath(const TYPath& path, const IAttributeDictionary& attributes);
    
    TRichYPath& operator = (const TRichYPath& other);

    const TYPath& GetPath() const;
    void SetPath(const TYPath& path);

    const IAttributeDictionary& Attributes() const;
    IAttributeDictionary& Attributes();

private:
    TYPath Path_;
    TAutoPtr<IAttributeDictionary> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRichYPath& path);
void Serialize(const TRichYPath& richPath, IYsonConsumer* consumer);
void Deserialize(TRichYPath& richPath, INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree 
} // namespace NYT
