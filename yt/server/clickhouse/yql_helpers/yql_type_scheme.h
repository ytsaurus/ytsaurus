#pragma once

#include <yql/ast/yql_expr.h>

#include <mapreduce/yt/node/node.h>

#include <library/yson/consumer.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NYql {

void WriteTypeToYson(NYT::TYsonConsumerBase& writer, const TTypeAnnotationNode* type);
void SaveStructTypeToYson(NYT::TYsonConsumerBase& writer, const TStructExprType* type, const TVector<TString>* columns = nullptr);
NYT::TNode TypeToYsonNode(const TTypeAnnotationNode* type);
TString WriteTypeToYson(const TTypeAnnotationNode* type);

const TTypeAnnotationNode* ParseTypeFromYson(const TString& yson, TExprContext& ctx, const TPosition& pos = {});
const TTypeAnnotationNode* ParseTypeFromYson(const NYT::TNode& node, TExprContext& ctx, const TPosition& pos = {});
bool ParseYson(NYT::TNode& res, const TString& yson, IOutputStream& err);

}
