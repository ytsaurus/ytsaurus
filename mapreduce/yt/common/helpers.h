#pragma once

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/yson/public.h>

class TInputStream;
class TOutputStream;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNode NodeFromYsonString(const Stroka& input, EYsonType type = YT_NODE);

TNode NodeFromJsonString(const Stroka& input);

Stroka NodeToYsonString(const TNode& node);

Stroka NodeToJsonString(const TNode& node);

Stroka NodeListToYsonString(const TNode::TList& nodes);

Stroka YPathToJsonString(const TRichYPath& path);

Stroka AttributesToJsonString(const TNode& attributes);

Stroka AttributeFilterToJsonString(const TAttributeFilter& filter);

void MergeNodes(TNode& dst, const TNode& src);

TYPath AddPathPrefix(const TYPath& path);

TRichYPath AddPathPrefix(const TRichYPath& path);

Stroka GetWriteTableCommand();
Stroka GetReadTableCommand();
Stroka GetWriteFileCommand();
Stroka GetReadFileCommand();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
