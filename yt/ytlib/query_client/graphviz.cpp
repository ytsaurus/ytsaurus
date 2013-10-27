#include "graphviz.h"

#ifndef _win_

#include "ast.h"
#include "ast_visitor.h"

#include "query_context.h"
#include "query_fragment.h"

#include <core/misc/assert.h>
#include <core/misc/proc.h>

#include <util/system/defaults.h>
#include <util/system/file.h>
#include <util/stream/file.h>

#include <sys/types.h>
#include <sys/wait.h>

// Required in printing visitor.
#include <core/misc/guid.h>
#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NDot {

////////////////////////////////////////////////////////////////////////////////

Stroka EscapeString(const Stroka& s)
{
    Stroka r(s.begin(), s.end());

    for (size_t i = 0; i < r.length(); ++i) {
        switch (r[i]) {
            case '\n':
                r.insert(r.begin() + i, '\\');
                ++i;
                r.replace(i, 1, 1, 'n'); // r[i] = 'n';
                break;
            case '\t':
                r.insert(r.begin() + i, ' ');
                ++i;
                r.replace(i, 1, 1, ' '); // r[i] = ' ';
            case '\\':
                if (i + 1 != r.length()) {
                    switch (r[i + 1]) {
                        case 'l': continue;
                        case '|':
                        case '{':
                        case '}':
                            r.erase(r.begin() + i);
                            continue;
                        default:
                            break;
                    }
                }
            case '{':
            case '}':
            case '<':
            case '>':
            case '|':
            case '"':
                r.insert(r.begin() + i, '\\');
                ++i;
                break;
        }
    }

    return r;
}

Stroka EscapeHtml(const Stroka& s)
{
    Stroka r(s.begin(), s.end());
    size_t i;

    auto replaceWith = [&] (const char* replacement) {
        r.erase(r.begin() + i);
        r.insert(i, replacement);
        i += strlen(replacement);
    };

    for (i = 0; i < r.length(); ++i) {
        switch (r[i]) {
            case '\n':
                replaceWith("<BR/>");
                break;
            case '<':
                replaceWith("&lt;");
                break;
            case '>':
                replaceWith("&gt;");
                break;
            case '&':
                replaceWith("&amp;");
                break;
        }
    }

    return r;
}

void ViewGraph(const Stroka& file)
{
    std::vector<Stroka> args;

    args.clear();
    args.push_back("xdot");
    args.push_back("-f");
    args.push_back("dot");
    args.push_back(file);

    int pid = Spawn("/usr/bin/xdot", args);
    int status = 0;
    int result = ::waitpid(pid, &status, WUNTRACED);

    YCHECK(result > 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDot
} // namespace NYT

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TGraphVizTraits;

template <class T>
struct TGraphVizTraits<
    T,
    typename std::enable_if<std::is_base_of<TOperator, T>::value>::type>
{
    static const int UniqueId = 1;
    static inline const char* GetPrefix() { return "Op"; }
};

template <class T>
struct TGraphVizTraits<
    T,
    typename std::enable_if<std::is_base_of<TExpression, T>::value>::type>
{
    static const int UniqueId = 2;
    static inline const char* GetPrefix() { return "Expr"; }
};

class TGraphVizVisitor
    : public IAstVisitor
{
public:
    TGraphVizVisitor(TOutputStream& output)
        : Output_(output)
    { }

    template <class TNode>
    Stroka GetName(TNode* node, const Stroka& port = "")
    {
        auto prefix = TGraphVizTraits<TNode>::GetPrefix();
        if (port.empty()) {
            return Sprintf("%s%p", prefix, node);
        } else {
            return Sprintf("%s%p:%s", prefix, node, port.c_str());
        }
    }

    void WriteHeader(const Stroka& title)
    {
        Output_ << "digraph unnamed {\n";
        Output_ << "\tlabel=\"" << NDot::EscapeString(title) << "\";\n";
        Output_ << "\tfontname=Courier;\n";
        Output_ << "\tnode [shape=none,colorscheme=pastel19,fontname=Courier];\n";
        Output_ << "\tedge [shape=solid,fontname=Courier];\n";
        Output_ << "\n";
    }

    void WriteFooter()
    {
        Output_ << "}\n";
    }

    template <class TNode>
    void WriteNode(TNode* node, const Stroka& label, const Stroka& attributes = "")
    {
        if (VisitedNodes_.find(node) == VisitedNodes_.end()) {
            VisitedNodes_.insert(node);
        } else {
            return;
        }

        Output_ << "\t" << GetName(node) << " [";
        if (!attributes.empty()) {
            Output_ << attributes << ",";
        }
        Output_ << "label=<" << label << ">];\n";
    }

    template <class TFrom, class TTo>
    void WriteEdge(
        TFrom* from,
        TTo* to,
        const Stroka& fromPort = "",
        const Stroka& toPort = "")
    {
        static const bool constrained =
            (TGraphVizTraits<TFrom>::UniqueId == TGraphVizTraits<TTo>::UniqueId);

        Output_ << "\t" << GetName(from, fromPort) << " -> " << GetName(to, toPort);
        if (!constrained) {
            Output_ << " [style=dotted,constraint=false]";
        }
        Output_ << ";\n";
    }

    struct TLabel
    {
        Stroka Value_;

        template <class TNode>
        TLabel(const TNode* node)
        {
            Value_ += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
            Value_ += Sprintf(
                "<TR><TD BGCOLOR=\"//%d\">%s</TD></TR>",
                TGraphVizTraits<TNode>::UniqueId,
                ~node->GetKind().ToString());
        }

        TLabel& WithRow(const Stroka& row)
        {
            Value_ += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
            Value_ += row;
            Value_ += "</TD></TR>";
            return *this;
        }

        TLabel& WithPortAndRow(const Stroka& port, const Stroka& row)
        {
            Value_ += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\" ";
            Value_ += "PORT=\"" + NDot::EscapeHtml(port) + "\">";
            Value_ += row;
            Value_ += "</TD></TR>";
            return *this;
        }

        Stroka Build()
        {
            Value_ += "</TABLE>";
            return Value_;
        }
    };

    virtual bool Visit(const TScanOperator* op) override
    {
        using NObjectClient::TObjectId;
        using NObjectClient::TypeFromId;
        auto objectId = NYT::FromProto<TObjectId>(op->DataSplit().chunk_id());
        WriteNode(
            op,
            TLabel(op)
                .WithRow(
                    "TableIndex: " + ToString(op->GetTableIndex()) +
                    "<BR/>Split: {" +
                    "<BR/>Id: " + ToString(objectId) +
                    "<BR/>Type: " + TypeFromId(objectId).ToString() +
                    "<BR/>}")
                .Build());
        return true;
    }

    virtual bool Visit(const TUnionOperator* op) override
    {
        WriteNode(op, TLabel(op).Build());
        for (const auto& source : op->Sources()) {
            WriteEdge(op, source);
        }
        return true;
    }

    virtual bool Visit(const TFilterOperator* op) override
    {
        WriteNode(
            op,
            TLabel(op)
                .WithPortAndRow("p",
                    "[P]: " + NDot::EscapeHtml(op->GetPredicate()->GetSource()))
                .Build());
        WriteEdge(op, op->GetSource());
        WriteEdge(op, op->GetPredicate(), "p");
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(const TProjectOperator* op) override
    {
        TLabel label(op);
        for (int i = 0; i < op->GetProjectionCount(); ++i) {
            label.WithPortAndRow(
                ToString(i),
                Sprintf("[%d]: ", i) + NDot::EscapeHtml(op->GetProjection(i)->GetSource()));
        }
        WriteNode(op, label.Build());
        WriteEdge(op, op->GetSource());
        for (int i = 0; i < op->GetProjectionCount(); ++i) {
            WriteEdge(op, op->GetProjection(i), ToString(i));
            Traverse(this, op->GetProjection(i));
        }
        return true;
    }

    virtual bool Visit(const TIntegerLiteralExpression* expr) override
    {
        WriteNode(
            expr,
            TLabel(expr).WithRow(ToString(expr->GetValue())).Build());
        return true;
    }

    virtual bool Visit(const TDoubleLiteralExpression* expr) override
    {
        WriteNode(
            expr,
            TLabel(expr).WithRow(ToString(expr->GetValue())).Build());
        return true;
    }

    virtual bool Visit(const TReferenceExpression* expr) override
    {
        WriteNode(
            expr,
            TLabel(expr)
                .WithRow(
                    "TableIndex: " + ToString(expr->GetTableIndex()) + "<BR/>" +
                    "Name: " + expr->GetName() + "<BR/>" +
                    "Type: " + expr->GetCachedType().ToString() + "<BR/>" +
                    "KeyIndex: " + ToString(expr->GetCachedKeyIndex()) + "<BR/>")
                .Build());
        return true;
    }

    virtual bool Visit(const TFunctionExpression* expr) override
    {
        TLabel label(expr);
        label.WithRow("Name: " + expr->GetName());
        for (int i = 0; i < expr->GetArgumentCount(); ++i) {
            label.WithPortAndRow(
                ToString(i),
                Sprintf("[%d]: ", i) + NDot::EscapeHtml(expr->GetArgument(i)->GetSource()));
        }
        WriteNode(expr, label.Build());
        for (int i = 0; i < expr->GetArgumentCount(); ++i) {
            WriteEdge(expr, expr->GetArgument(i), ToString(i));
            Traverse(this, expr->GetArgument(i));
        }
        return true;
    }

    virtual bool Visit(const TBinaryOpExpression* expr) override
    {
        WriteNode(
            expr,
            TLabel(expr)
                .WithRow("OpCode: " + expr->GetOpcode().ToString())
                .Build());
        WriteEdge(expr, expr->GetLhs());
        WriteEdge(expr, expr->GetRhs());
        return true;
    }

private:
    TOutputStream& Output_;
    std::unordered_set<const void*> VisitedNodes_;

};

////////////////////////////////////////////////////////////////////////////////

void ViewFragment(const TQueryFragment& fragment, const Stroka& title_)
{
    char name[] = "/tmp/graph.XXXXXX";
    int fd = mkstemp(name);

    YCHECK(fd > 0);

    auto debugInformation = fragment.GetContext()->GetDebugInformation();
    auto headOperator = fragment.GetHead();

    try {
        TFile handle(fd);
        TFileOutput output(handle);

        TGraphVizVisitor visitor(output);

        auto title = title_;
        if (title.empty()) {
            title = debugInformation ? debugInformation->Source : "";
        }

        visitor.WriteHeader(title);
        Traverse(&visitor, headOperator);
        visitor.WriteFooter();

        NDot::ViewGraph(name);
        ::unlink(name);
    } catch (...) {
        ::unlink(name);
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#else

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void ViewFragment(const TQueryFragment& /*fragment*/, const Stroka& /*title*/)
{
    YUNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#endif

