#include "collect_tables.h"

#include "collect.h"

#define USE_CURRENT_UDF_ABI_VERSION
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

    THashMap<TString, THashMap<TString, TVector<TFolderEntry>>>
    CollectTables(const NYql::TExprNode& root) {
        THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> tablesByCluster;
        NYql::VisitExpr(root, [&](const NYql::TExprNode& node) -> bool {
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

            TFolderEntry entry = {
                .Type = TFolderEntry::Table,
                .Name = std::move(*table),
            };

            tablesByCluster[std::move(cluster)]["/"].emplace_back(std::move(entry));
            return true;
        });
        for (auto& [_, tree] : tablesByCluster) {
            SortUnique(tree["/"]);
        }
        return tablesByCluster;
    }

} // namespace NSQLComplete
