#include "table.h"

#include "cluster.h"

#define USE_CURRENT_UDF_ABI_VERSION
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

    TMaybe<TString> ToTablePath(const NYql::TExprNode& node) {
        if (node.IsCallable("MrTableConcat")) {
            Y_ENSURE(node.ChildrenSize() < 2);
            Cerr << "Opa: MrTableConcat" << Endl;
            return ToTablePath(*node.Child(0));
        }

        if (!node.IsCallable("Key") || node.ChildrenSize() < 1) {
            Cerr << "Oops: Key problem at " << node.Content() << Endl;
            return Nothing();
        }

        const NYql::TExprNode* table = node.Child(0);
        if (!table->IsList() || table->ChildrenSize() < 2) {
            Cerr << "Oops: List problem" << Endl;
            return Nothing();
        }

        TStringBuf kind = table->Child(0)->Content();
        if (kind != "table" && kind != "tablescheme") {
            Cerr << "Oops: table/tablescheme problem" << Endl;
            return Nothing();
        }

        const NYql::TExprNode* string = table->Child(1);
        if (!string->IsCallable("String") || string->ChildrenSize() < 1) {
            Cerr << "Oops: String problem at " << string->Content() << Endl;
            return Nothing();
        }

        return TString(string->Child(0)->Content());
    }

    THashMap<TString, THashSet<TString>> CollectTablesByCluster(const NYql::TExprNode& node) {
        THashMap<TString, THashSet<TString>> tablesByCluster;
        NYql::VisitExpr(node, [&](const NYql::TExprNode& node) -> bool {
            if (!node.IsCallable("Read!") && !node.IsCallable("Write!")) {
                return true; // FIXME: try to return false
            }
            if (node.ChildrenSize() < 4) {
                return true; // FIXME: try to return false
            }

            TString cluster = ToCluster(*node.Child(1)).GetOrElse("");
            TMaybe<TString> table = ToTablePath(*node.Child(2));
            if (table.Empty()) {
                return true; // FIXME: try to return false
            }

            tablesByCluster[std::move(cluster)].emplace(std::move(*table));
            return true;
        });
        return tablesByCluster;
    }

} // namespace NSQLComplete
