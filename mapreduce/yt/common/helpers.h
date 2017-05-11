#pragma once

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/interface/common.h>
#include <library/yson/public.h>

class TInputStream;
class TOutputStream;

namespace NJson {
    class TJsonValue;
} // namespace NJson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNode NodeFromYsonString(const TString& input, EYsonType type = YT_NODE);
TString NodeToYsonString(const TNode& node, EYsonFormat format = YF_TEXT);

// TODO: EYsonType argument should be removed since it doesn't affect anything
// (check unittest MakeSureThatSecondParamDoesntAffectAnything)
TNode NodeFromJsonString(const TString& input, EYsonType type = YT_NODE);
TNode NodeFromJsonValue(const NJson::TJsonValue& input);

TString NodeListToYsonString(const TNode::TList& nodes);

TNode NodeFromYPath(const TRichYPath& path);

TString AttributesToYsonString(const TNode& attributes);

TString AttributeFilterToYsonString(const TAttributeFilter& filter);

TNode NodeFromTableSchema(const TTableSchema& schema);

void MergeNodes(TNode& dst, const TNode& src);

TYPath AddPathPrefix(const TYPath& path);

TString GetWriteTableCommand();
TString GetReadTableCommand();
TString GetWriteFileCommand();
TString GetReadFileCommand();

bool IsTrivial(const TReadLimit& readLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
