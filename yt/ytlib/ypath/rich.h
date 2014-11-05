#pragma once

#include "public.h"

#include <core/misc/serialize.h>

#include <core/yson/public.h>

#include <core/ytree/attributes.h>

#include <ytlib/chunk_client/read_limit.h>

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
    TRichYPath Normalize() const;

    const TYPath& GetPath() const;
    void SetPath(const TYPath& path);

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    // Attribute accessors.
    bool GetAppend() const;
    NChunkClient::TChannel GetChannel() const;
    std::vector<NChunkClient::TReadRange> GetRanges() const;

private:
    TYPath Path_;
    std::unique_ptr<NYTree::IAttributeDictionary> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRichYPath& path);

std::vector<TRichYPath> Normalize(const std::vector<TRichYPath>& paths);

void InitializeFetchRequest(
    NChunkClient::NProto::TReqFetch* request,
    const TRichYPath& richPath);

void Serialize(const TRichYPath& richPath, NYson::IYsonConsumer* consumer);
void Deserialize(TRichYPath& richPath, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
