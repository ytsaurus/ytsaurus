#pragma once

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/interface/common.h>
#include <library/yson/public.h>

class TInputStream;
class TOutputStream;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNode NodeFromYsonString(const Stroka& input, EYsonType type = YT_NODE);

Stroka NodeToYsonString(const TNode& node, EYsonFormat format = YF_TEXT);

Stroka NodeListToYsonString(const TNode::TList& nodes);

TNode NodeFromYPath(const TRichYPath& path);

Stroka AttributesToYsonString(const TNode& attributes);

Stroka AttributeFilterToYsonString(const TAttributeFilter& filter);

void MergeNodes(TNode& dst, const TNode& src);

TYPath AddPathPrefix(const TYPath& path);

Stroka GetWriteTableCommand();
Stroka GetReadTableCommand();
Stroka GetWriteFileCommand();
Stroka GetReadFileCommand();

bool IsTrivial(const TReadLimit& readLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
