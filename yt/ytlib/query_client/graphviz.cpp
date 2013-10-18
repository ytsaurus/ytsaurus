#include "graphviz.h"

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
    static const int Id = 1;
    static inline const char* GetKind() { return "Op"; }
};

template <class T>
struct TGraphVizTraits<
    T,
    typename std::enable_if<std::is_base_of<TExpression, T>::value>::type>
{
    static const int Id = 2;
    static inline const char* GetKind() { return "Expr"; }
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
        auto kind = TGraphVizTraits<TNode>::GetKind();
        if (port.empty()) {
            return Sprintf("%s%p", kind, node);
        } else {
            return Sprintf("%s%p:%s", kind, node, port.c_str());
        }
    }

    void WriteHeader(const Stroka& query)
    {
        Output_ << "digraph unnamed {\n";
        Output_ << "\tlabel=\"" << NDot::EscapeString(query) << "\";\n";
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
    void WriteNode(TNode* op, const Stroka& label, const Stroka& attributes = "")
    {
        Output_ << "\t" << GetName(op) << " [";
        if (!attributes.empty()) {
            Output_ << attributes << ",";
        }
        Output_ << "label=<" << label << ">];\n";

        for (auto it = op->ChildBegin(); it != op->ChildEnd(); ++it) {
            WriteEdge(op, *it);
        }
    }

    template <class TFrom, class TTo>
    void WriteEdge(
        TFrom* from,
        TTo* to,
        const Stroka& fromPort = "",
        const Stroka& toPort = "")
    {
        static const bool constrained =
            (TGraphVizTraits<TFrom>::Id == TGraphVizTraits<TTo>::Id);

        Output_ << "\t" << GetName(from, fromPort) << " -> " << GetName(to, toPort);
        if (!constrained) {
            Output_ << " [style=dotted,constraint=false]";
        }
        Output_ << ";\n";
    }

    virtual bool Visit(TScanOperator* op) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//1\">Scan</TD></TR>";
        label += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
        label += "TableIndex: " + ToString(op->GetTableIndex());
        label += "<BR/>Split: {<BR/>";
        label += NDot::EscapeHtml(op->DataSplit().DebugString());
        label += "<BR/>}";
        label += "</TD></TR>";
        label += "</TABLE>";

        WriteNode(op, label);
        return true;
    }

    virtual bool Visit(TFilterOperator* op) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//1\">Filter</TD></TR>";
        label += "<TR><TD PORT=\"p\" ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
        label += "[P]: " + NDot::EscapeHtml(op->GetPredicate()->GetSource());
        label += "</TD></TR>";
        label += "</TABLE>";

        WriteNode(op, label);
        WriteEdge(op, op->GetPredicate(), "p");
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(TProjectOperator* op) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//1\">Project</TD></TR>";
        for (int i = 0; i < op->Expressions().size(); ++i) {
            label += "<TR><TD PORT=\"" + ToString(i) + "\" ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
            label += Sprintf("[%d]: ", i) + NDot::EscapeHtml(op->GetExpression(i)->GetSource());
            label += "</TD></TR>";
        }
        label += "</TABLE>";

        WriteNode(op, label);
        for (int i = 0; i < op->Expressions().size(); ++i) {
            WriteEdge(op, op->GetExpression(i), ToString(i));
            Traverse(this, op->GetExpression(i));
        }
        return true;
    }

    virtual bool Visit(TIntegerLiteralExpression* expr) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//2\">IntegerLiteral</TD></TR>";
        label += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
        label += ToString(expr->GetValue());
        label += "</TD></TR>";
        label += "</TABLE>";

        WriteNode(expr, label);
        return true;
    }

    virtual bool Visit(TDoubleLiteralExpression* expr) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//2\">DoubleLiteral</TD></TR>";
        label += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
        label += ToString(expr->GetValue());
        label += "</TD></TR>";
        label += "</TABLE>";

        WriteNode(expr, label);
        return true;
    }

    virtual bool Visit(TReferenceExpression* expr) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//2\">Reference</TD></TR>";
        label += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
        label += "TableIndex: " + ToString(expr->GetTableIndex()) + "<BR/>";
        label += "Name: " + ToString(expr->GetName()) + "<BR/>";
        label += "Type: " + expr->GetType().ToString() + "<BR/>";
        label += "</TD></TR>";
        label += "</TABLE>";

        WriteNode(expr, label);
        return true;
    }

    virtual bool Visit(TFunctionExpression* expr) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//2\">Function</TD></TR>";
        label += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
        label += ToString(expr->GetName());
        label += "</TD></TR>";
        label += "</TABLE>";

        YUNREACHABLE();
        return true;
    }

    virtual bool Visit(TBinaryOpExpression* expr) override
    {
        Stroka label;
        label += "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">";
        label += "<TR><TD BGCOLOR=\"//2\">BinaryOp</TD></TR>";
        label += "<TR><TD ALIGN=\"LEFT\" BALIGN=\"LEFT\">";
        label += "OpCode: " + expr->GetOpcode().ToString();
        label += "</TD></TR>";
        label += "</TABLE>";

        WriteNode(expr, label);
        return true;
    }

private:
    TOutputStream& Output_;

};

////////////////////////////////////////////////////////////////////////////////

void ViewFragment(TQueryFragment* fragment)
{
    char name[] = "/tmp/graph.XXXXXX";
    int fd = mkstemp(name);

    YCHECK(fd > 0);

    auto debugInformation = fragment->GetContext()->GetDebugInformation();
    auto headOperator = fragment->GetHead();

    try {
        TFile handle(fd);
        TFileOutput output(handle);

        TGraphVizVisitor visitor(output);

        visitor.WriteHeader(debugInformation ? debugInformation->Source : "");
        Traverse(&visitor, headOperator);
        visitor.WriteFooter();

        NDot::ViewGraph(name);
        ::unlink(name);
    } catch (...) {
        throw;
    }


}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

