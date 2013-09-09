#pragma once

#include "public.h"

#include <core/misc/serialize.h>

#include <core/yson/public.h>

#include <core/ytree/attributes.h>

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

    static TRichYPath Parse(const Stroka& str);
    TRichYPath Simplify() const;

    const TYPath& GetPath() const;
    void SetPath(const TYPath& path);

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    TYPath Path_;
    std::unique_ptr<NYTree::IAttributeDictionary> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRichYPath& path);

std::vector<TRichYPath> Simplify(const std::vector<TRichYPath>& paths);

void Serialize(const TRichYPath& richPath, NYson::IYsonConsumer* consumer);
void Deserialize(TRichYPath& richPath, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
