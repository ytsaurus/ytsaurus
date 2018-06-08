#include "arithmetic_formula.h"
#include "phoenix.h"

#include <yt/core/misc/error.h>

#include <yt/core/yson/string.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node.h>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEvaluationContext,
    (Boolean)
    (Arithmetic)
);

bool IsSymbolAllowedInName(char c, EEvaluationContext context, bool isFirst)
{
    const static THashSet<char> extraAllowedBooleanVariableTokens{'/', '-'};
    if (std::isalpha(c) || c == '_') {
        return true;
    }
    if (isFirst) {
        return false;
    }
    if (std::isdigit(c)) {
        return true;
    }
    if (context == EEvaluationContext::Boolean && extraAllowedBooleanVariableTokens.has(c)) {
        return true;
    }
    return false;
}

void ValidateFormulaVariable(const TString& variable, EEvaluationContext context)
{
    if (variable.empty()) {
        THROW_ERROR_EXCEPTION("Variable should not be empty");
    }
    for (char c : variable) {
        if (!IsSymbolAllowedInName(c, context, false)) {
            THROW_ERROR_EXCEPTION("Invalid character %Qv in variable %Qv", c, variable);
        }
    }
    if (!IsSymbolAllowedInName(variable[0], context, true)) {
        THROW_ERROR_EXCEPTION("Invalid first character in variable: %Qv", variable);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateArithmeticFormulaVariable(const TString& variable)
{
    ValidateFormulaVariable(variable, EEvaluationContext::Arithmetic);
}

void ValidateBooleanFormulaVariable(const TString& variable)
{
    ValidateFormulaVariable(variable, EEvaluationContext::Boolean);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void ThrowError(const TString& formula, int position, const TString& message)
{
    const static int maxContextSize = 30;

    int contextStart = std::max(0, position - maxContextSize / 2);
    TString context = formula.substr(contextStart, maxContextSize);
    int contextPosition = std::min(position, maxContextSize / 2);

    TStringBuilder builder;
    Format(&builder, "Error while parsing arithmetic formula:\n%v\n", formula);
    builder.AppendChar(' ', position);
    Format(&builder, "^\n%v", message);
    THROW_ERROR_EXCEPTION(builder.Flush())
        << TErrorAttribute("context", context)
        << TErrorAttribute("context_pos", contextPosition);
}

#define FOR_EACH_TOKEN(func) \
    func(0, Variable) \
    func(0, Integer) \
    func(0, LeftBracket) \
    func(0, RightBracket) \
    func(1, LogicalOr) \
    func(2, LogicalAnd) \
    func(3, BitwiseOr) \
    func(4, BitwiseXor) \
    func(5, BitwiseAnd) \
    func(6, Equals) \
    func(6, NotEquals) \
    func(7, Less) \
    func(7, Greater) \
    func(7, LessOrEqual) \
    func(7, GreaterOrEqual) \
    func(8, Plus) \
    func(8, Minus) \
    func(9, Multiplies) \
    func(9, Divides) \
    func(9, Modulus) \
    func(10, LogicalNot)

#define EXTRACT_FIELD_NAME(x, y) (y)
#define EXTRACT_PRECEDENCE(x, y) x,

DEFINE_ENUM(EFormulaTokenType,
        FOR_EACH_TOKEN(EXTRACT_FIELD_NAME)
);

static int Precedence(EFormulaTokenType type) {
    constexpr static int precedence[] =
    {
        FOR_EACH_TOKEN(EXTRACT_PRECEDENCE)
    };
    int index = static_cast<int>(type);
    YCHECK(0 <= index && index < sizeof(precedence) / sizeof(*precedence));
    return precedence[index];
}

#undef FOR_EACH_TOKEN
#undef EXTRACT_FIELD_NAME
#undef EXTRACT_PRECEDENCE

struct TFormulaToken
{
    EFormulaTokenType Type;
    int Position;
    TString Name;
    i64 Number = 0;
};

TString ToString(const TFormulaToken& token)
{
    if (token.Type == EFormulaTokenType::Integer) {
        return ToString(token.Number);
    } else if (token.Type == EFormulaTokenType::Variable) {
        return "[" + token.Name + "]";
    } else {
        return TEnumTraitsImpl_EFormulaTokenType::GetDomainNames()[
            static_cast<int>(token.Type)];
    }
}

bool operator==(const TFormulaToken& lhs, const TFormulaToken& rhs)
{
    return lhs.Type == rhs.Type && lhs.Name == rhs.Name && lhs.Number == rhs.Number;
}

bool operator!=(const TFormulaToken& lhs, const TFormulaToken& rhs)
{
    return !(lhs == rhs);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TGenericFormulaImpl
    : public TIntrinsicRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TString, Formula);
    DEFINE_BYVAL_RO_PROPERTY(size_t, Hash);

public:
    TGenericFormulaImpl(const TString& formula, size_t hash, std::vector<TFormulaToken> parsedFormula);

    bool operator==(const TGenericFormulaImpl& other) const;

    bool IsEmpty() const;

    int Size() const;

    i64 Eval(const THashMap<TString, i64>& values, EEvaluationContext context) const;

    THashSet<TString> GetVariables() const;

private:
    std::vector<TFormulaToken> ParsedFormula_;

    static std::vector<TFormulaToken> Tokenize(const TString& formula, EEvaluationContext context);
    static std::vector<TFormulaToken> Parse(
        const TString& formula,
        const std::vector<TFormulaToken>& tokens,
        EEvaluationContext context);
    static size_t CalculateHash(const std::vector<TFormulaToken> tokens);

    friend TIntrusivePtr<TGenericFormulaImpl> MakeGenericFormulaImpl(const TString& formula, EEvaluationContext context);
};

////////////////////////////////////////////////////////////////////////////////

TGenericFormulaImpl::TGenericFormulaImpl(
    const TString& formula,
    size_t hash,
    std::vector<TFormulaToken> parsedFormula)
    : Formula_(formula)
    , Hash_(hash)
    , ParsedFormula_(std::move(parsedFormula))
{ }

bool TGenericFormulaImpl::operator==(const TGenericFormulaImpl& other) const
{
    return std::equal(
        ParsedFormula_.begin(),
        ParsedFormula_.end(),
        other.ParsedFormula_.begin(),
        other.ParsedFormula_.end());
}

bool TGenericFormulaImpl::IsEmpty() const
{
    return ParsedFormula_.empty();
}

int TGenericFormulaImpl::Size() const
{
    return ParsedFormula_.size();
}

i64 TGenericFormulaImpl::Eval(const THashMap<TString, i64>& values, EEvaluationContext context) const
{
    auto variableValue = [&] (const TString& var) -> i64 {
        auto iter = values.find(var);
        if (iter == values.end()) {
            if (context == EEvaluationContext::Boolean) {
                return 0;
            } else {
                THROW_ERROR_EXCEPTION("Undefined variable %Qv", var)
                    << TErrorAttribute("formula", Formula_)
                    << TErrorAttribute("values", values);
            }
        }
        return iter->second;
    };

#define APPLY_BINARY_OP(op) \
    YCHECK(stack.size() >= 2); \
    { \
        i64 top = stack.back(); \
        stack.pop_back(); \
        stack.back() = stack.back() op top; \
    }

    std::vector<i64> stack;
    for (const auto& token : ParsedFormula_) {
        switch (token.Type) {
            case EFormulaTokenType::Variable:
                stack.push_back(variableValue(token.Name));
                break;
            case EFormulaTokenType::Integer:
                stack.push_back(token.Number);
                break;
            case EFormulaTokenType::LogicalNot:
                YCHECK(!stack.empty());
                stack.back() = !stack.back();
                break;
            case EFormulaTokenType::LogicalOr:
                APPLY_BINARY_OP(||);
                break;
            case EFormulaTokenType::LogicalAnd:
                APPLY_BINARY_OP(&&);
                break;
            case EFormulaTokenType::BitwiseOr:
                APPLY_BINARY_OP(|);
                break;
            case EFormulaTokenType::BitwiseXor:
                APPLY_BINARY_OP(^);
                break;
            case EFormulaTokenType::BitwiseAnd:
                APPLY_BINARY_OP(&);
                break;
            case EFormulaTokenType::Equals:
                APPLY_BINARY_OP(==);
                break;
            case EFormulaTokenType::NotEquals:
                APPLY_BINARY_OP(!=);
                break;
            case EFormulaTokenType::Less:
                APPLY_BINARY_OP(<);
                break;
            case EFormulaTokenType::Greater:
                APPLY_BINARY_OP(>);
                break;
            case EFormulaTokenType::LessOrEqual:
                APPLY_BINARY_OP(<=);
                break;
            case EFormulaTokenType::GreaterOrEqual:
                APPLY_BINARY_OP(>=);
                break;
            case EFormulaTokenType::Plus:
                APPLY_BINARY_OP(+);
                break;
            case EFormulaTokenType::Minus:
                APPLY_BINARY_OP(-);
                break;
            case EFormulaTokenType::Multiplies:
                APPLY_BINARY_OP(*);
                break;
            case EFormulaTokenType::Divides:
            case EFormulaTokenType::Modulus:
                YCHECK(stack.size() >= 2);
                {
                    i64 top = stack.back();
                    if (top == 0) {
                        THROW_ERROR_EXCEPTION("Division by zero in formula: %Qv", Formula_)
                            << TErrorAttribute("values", values);
                    }
                    stack.pop_back();
                    if (token.Type == EFormulaTokenType::Divides) {
                        stack.back() = stack.back() / top;
                    } else {
                        stack.back() = stack.back() % top;
                    }
                }
                break;
            default:
                Y_UNREACHABLE();
        }
    }
    if (stack.empty()) {
        if (context == EEvaluationContext::Arithmetic) {
            THROW_ERROR_EXCEPTION("Empty arithmetic formula cannot be evaluated");
        }
        return true;
    }
    YCHECK(stack.size() == 1);
    return stack[0];

#undef APPLY_BINARY_OP
}

THashSet<TString> TGenericFormulaImpl::GetVariables() const
{
    THashSet<TString> variables;
    for (const auto& token : ParsedFormula_) {
        if (token.Type == EFormulaTokenType::Variable) {
            variables.insert(token.Name);
        }
    }
    return variables;
}

std::vector<TFormulaToken> TGenericFormulaImpl::Tokenize(const TString& formula, EEvaluationContext context)
{
    std::vector<TFormulaToken> result;
    size_t pos = 0;

    auto throwError = [&] (int position, const TString& message) {
        ThrowError(formula, position, message);
    };

    auto skipWhitespace = [&] {
        while (pos < formula.size() && std::isspace(formula[pos])) {
            ++pos;
        }
    };

    auto extractSpecialToken = [&] {
        char first = formula[pos];
        char second = pos + 1 < formula.Size() ? formula[pos + 1] : '\0';
        switch (first) {
            case '^':
                ++pos;
                return EFormulaTokenType::BitwiseXor;
            case '+':
                ++pos;
                return EFormulaTokenType::Plus;
            case '-':
                ++pos;
                return EFormulaTokenType::Minus;
            case '*':
                ++pos;
                return EFormulaTokenType::Multiplies;
            case '/':
                ++pos;
                return EFormulaTokenType::Divides;
            case '%':
                ++pos;
                return EFormulaTokenType::Modulus;
            case '(':
                ++pos;
                return EFormulaTokenType::LeftBracket;
            case ')':
                ++pos;
                return EFormulaTokenType::RightBracket;
            case '=':
                if (second != '=') {
                    throwError(pos + 1, "Unexpected character");
                }
                pos += 2;
                return EFormulaTokenType::Equals;
            case '!':
                switch (second) {
                    case '=':
                        pos += 2;
                        return EFormulaTokenType::NotEquals;
                    default:
                        ++pos;
                        return EFormulaTokenType::LogicalNot;
                }
            case '&':
                switch (second) {
                    case '&':
                        pos += 2;
                        return EFormulaTokenType::LogicalAnd;
                    default:
                        ++pos;
                        return EFormulaTokenType::BitwiseAnd;
                }
            case '|':
                switch (second) {
                    case '|':
                        pos += 2;
                        return EFormulaTokenType::LogicalOr;
                    default:
                        ++pos;
                        return EFormulaTokenType::BitwiseOr;
                }
            case '<':
                switch (second) {
                    case '=':
                        pos += 2;
                        return EFormulaTokenType::LessOrEqual;
                    default:
                        ++pos;
                        return EFormulaTokenType::Less;
                }
            case '>':
                switch (second) {
                    case '=':
                        pos += 2;
                        return EFormulaTokenType::GreaterOrEqual;
                    default:
                        ++pos;
                        return EFormulaTokenType::Greater;
                }
            default:
                throwError(pos, "Unexpected character");
                Y_UNREACHABLE();
        }
    };

    auto extractInteger = [&] {
        TString buf;
        if (formula[pos] == '-') {
            buf += formula[pos++];
        }
        if (pos == formula.Size() || !std::isdigit(formula[pos])) {
            throwError(pos, "Expected digit");
        }
        while (pos < formula.Size() && std::isdigit(formula[pos])) {
            buf += formula[pos++];
        }
        return IntFromString<i64, 10>(buf);
    };

    auto extractVariable = [&] {
        TString name;
        while (pos < formula.Size() && IsSymbolAllowedInName(formula[pos], context, name.empty())) {
            name += formula[pos++];
        }
        return name;
    };

    bool expectBinaryOperator = false;

    while (pos < formula.Size()) {
        char c = formula[pos];
        if (std::isspace(c)) {
            skipWhitespace();
            if (pos == formula.Size()) {
                break;
            }
            c = formula[pos];
        }

        TFormulaToken token;
        token.Position = pos;

        if (std::isdigit(c) || (c == '-' && !expectBinaryOperator)) {
            token.Type = EFormulaTokenType::Integer;
            token.Number = extractInteger();
            expectBinaryOperator = true;
        } else if (IsSymbolAllowedInName(formula[pos], context, true)) {
            token.Type = EFormulaTokenType::Variable;
            token.Name = extractVariable();
            expectBinaryOperator = true;
        } else {
            token.Type = extractSpecialToken();
            if (token.Type == EFormulaTokenType::RightBracket) {
                expectBinaryOperator = true;
            } else {
                expectBinaryOperator = false;
            }
        }

        result.push_back(token);
    }

    return result;
}

std::vector<TFormulaToken> TGenericFormulaImpl::Parse(
    const TString& formula,
    const std::vector<TFormulaToken>& tokens,
    EEvaluationContext context)
{
    std::vector<TFormulaToken> result;
    std::vector<TFormulaToken> stack;
    bool expectSubformula = true;

    if (tokens.empty()) {
        return result;
    }

    auto throwError = [&] (int position, const TString& message) {
        ThrowError(formula, position, message);
    };

    auto finishSubformula = [&] () {
        while (!stack.empty() && stack.back().Type != EFormulaTokenType::LeftBracket) {
            result.push_back(stack.back());
            stack.pop_back();
        }
    };

    auto processBinaryOp = [&] (const TFormulaToken& token) {
        while (!stack.empty() && Precedence(stack.back().Type) >= Precedence(token.Type)) {
            result.push_back(stack.back());
            stack.pop_back();
        }
    };

    for (const auto& token : tokens) {
        switch (token.Type) {
            case EFormulaTokenType::Variable:
            case EFormulaTokenType::Integer:
                if (!expectSubformula) {
                    throwError(token.Position, "Unexpected variable");
                }
                result.push_back(token);
                expectSubformula = false;
                break;

            case EFormulaTokenType::LogicalNot:
            case EFormulaTokenType::LeftBracket:
                if (!expectSubformula) {
                    throwError(token.Position, "Unexpected token");
                }
                stack.push_back(token);
                break;

            case EFormulaTokenType::RightBracket:
                if (expectSubformula) {
                    throwError(token.Position, "Unexpected token");
                }
                finishSubformula();
                if (stack.empty()) {
                    throwError(token.Position, "Unmatched ')'");
                }
                stack.pop_back();
                break;

            default:
                if (expectSubformula) {
                    throwError(token.Position, "Unexpected token");
                }
                processBinaryOp(token);
                stack.push_back(token);
                expectSubformula = true;
                break;
        }
    }

    if (expectSubformula) {
        throwError(formula.Size(), "Unfinished formula");
    }
    finishSubformula();
    if (!stack.empty()) {
        throwError(stack.back().Position, "Unmatched '('");
    }

    if (context == EEvaluationContext::Boolean) {
        for (const auto& token : result) {
            switch (token.Type) {
                case EFormulaTokenType::BitwiseAnd:
                case EFormulaTokenType::BitwiseOr:
                case EFormulaTokenType::LogicalNot:
                case EFormulaTokenType::Variable:
                    break;
                default:
                    throwError(token.Position, "Invalid token in boolean formula (only '!', '&', '|', '(', ')' are allowed)");
                    Y_UNREACHABLE();
            }
        }
    }

    return result;
}

size_t TGenericFormulaImpl::CalculateHash(const std::vector<TFormulaToken> tokens)
{
    size_t result = 0x18a92ea497f9bb1e;

    for (const auto& token : tokens) {
        HashCombine(result, static_cast<size_t>(token.Type));
        HashCombine(result, token.Name.hash());
        HashCombine(result, static_cast<size_t>(token.Number));
    }

    return result;
}

TIntrusivePtr<TGenericFormulaImpl> MakeGenericFormulaImpl(const TString& formula, EEvaluationContext context)
{
    auto tokens = TGenericFormulaImpl::Tokenize(formula, context);
    auto parsed = TGenericFormulaImpl::Parse(formula, tokens, context);
    auto hash = TGenericFormulaImpl::CalculateHash(parsed);
    return New<TGenericFormulaImpl>(formula, hash, parsed);
}

////////////////////////////////////////////////////////////////////////////////

TArithmeticFormula::TArithmeticFormula()
    : Impl_(MakeGenericFormulaImpl(TString(), EEvaluationContext::Arithmetic))
{ }

TArithmeticFormula::TArithmeticFormula(TIntrusivePtr<TGenericFormulaImpl> impl)
    : Impl_(std::move(impl))
{ }

TArithmeticFormula::TArithmeticFormula(const TArithmeticFormula& other) = default;
TArithmeticFormula::TArithmeticFormula(TArithmeticFormula&& other) = default;
TArithmeticFormula& TArithmeticFormula::operator=(const TArithmeticFormula& other) = default;
TArithmeticFormula& TArithmeticFormula::operator=(TArithmeticFormula&& other) = default;
TArithmeticFormula::~TArithmeticFormula() = default;

bool TArithmeticFormula::operator==(const TArithmeticFormula& other) const
{
    return *Impl_ == *other.Impl_;
}

bool TArithmeticFormula::IsEmpty() const
{
    return Impl_->IsEmpty();
}

int TArithmeticFormula::Size() const
{
    return Impl_->Size();
}

size_t TArithmeticFormula::GetHash() const
{
    return Impl_->GetHash();
}

TString TArithmeticFormula::GetFormula() const
{
    return Impl_->GetFormula();
}

i64 TArithmeticFormula::Eval(const THashMap<TString, i64>& values) const
{
    return Impl_->Eval(values, EEvaluationContext::Arithmetic);
}

THashSet<TString> TArithmeticFormula::GetVariables() const
{
    return Impl_->GetVariables();
}

TArithmeticFormula MakeArithmeticFormula(const TString& formula)
{
    auto impl = MakeGenericFormulaImpl(formula, EEvaluationContext::Arithmetic);
    return TArithmeticFormula(std::move(impl));
}

void Serialize(const TArithmeticFormula& arithmeticFormula, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(arithmeticFormula.GetFormula());
}

void Deserialize(TArithmeticFormula& arithmeticFormula, NYTree::INodePtr node)
{
    arithmeticFormula = MakeArithmeticFormula(node->AsString()->GetValue());
}

void TArithmeticFormula::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, GetFormula());
}

void TArithmeticFormula::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    auto formula = Load<TString>(context);
    Impl_ = MakeGenericFormulaImpl(formula, EEvaluationContext::Arithmetic);
}

////////////////////////////////////////////////////////////////////////////////

TBooleanFormula::TBooleanFormula()
    : Impl_(MakeGenericFormulaImpl(TString(), EEvaluationContext::Boolean))
{ }

TBooleanFormula::TBooleanFormula(TIntrusivePtr<TGenericFormulaImpl> impl)
    : Impl_(std::move(impl))
{ }

TBooleanFormula::TBooleanFormula(const TBooleanFormula& other) = default;
TBooleanFormula::TBooleanFormula(TBooleanFormula&& other) = default;
TBooleanFormula& TBooleanFormula::operator=(const TBooleanFormula& other) = default;
TBooleanFormula& TBooleanFormula::operator=(TBooleanFormula&& other) = default;
TBooleanFormula::~TBooleanFormula() = default;

bool TBooleanFormula::operator==(const TBooleanFormula& other) const
{
    return *Impl_ == *other.Impl_;
}

bool TBooleanFormula::IsEmpty() const
{
    return Impl_->IsEmpty();
}

int TBooleanFormula::Size() const
{
    return Impl_->Size();
}

size_t TBooleanFormula::GetHash() const
{
    return Impl_->GetHash();
}

TString TBooleanFormula::GetFormula() const
{
    return Impl_->GetFormula();
}

bool TBooleanFormula::IsSatisfiedBy(const std::vector<TString>& value) const
{
    THashMap<TString, i64> values;
    for (const auto& key: value) {
        values[key] = 1;
    }
    return Impl_->Eval(values, EEvaluationContext::Boolean);
}

bool TBooleanFormula::IsSatisfiedBy(const THashSet<TString>& value) const
{
    return IsSatisfiedBy(std::vector<TString>(value.begin(), value.end()));
}

TBooleanFormula MakeBooleanFormula(const TString& formula)
{
    auto impl = MakeGenericFormulaImpl(formula, EEvaluationContext::Boolean);
    return TBooleanFormula(std::move(impl));
}

void Serialize(const TBooleanFormula& booleanFormula, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(booleanFormula.GetFormula());
}

void Deserialize(TBooleanFormula& booleanFormula, NYTree::INodePtr node)
{
    booleanFormula = MakeBooleanFormula(node->AsString()->GetValue());
}

void TBooleanFormula::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, GetFormula());
}

void TBooleanFormula::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    auto formula = Load<TString>(context);
    Impl_ = MakeGenericFormulaImpl(formula, EEvaluationContext::Boolean);
}

////////////////////////////////////////////////////////////////////////////////

TTimeFormula::TTimeFormula() = default;
TTimeFormula::TTimeFormula(const TTimeFormula& other) = default;
TTimeFormula::TTimeFormula(TTimeFormula&& other) = default;
TTimeFormula& TTimeFormula::operator=(const TTimeFormula& other) = default;
TTimeFormula& TTimeFormula::operator=(TTimeFormula&& other) = default;
TTimeFormula::~TTimeFormula() = default;

bool TTimeFormula::operator==(const TTimeFormula& other) const
{
    return Formula_ == other.Formula_;
}

bool TTimeFormula::IsEmpty() const
{
    return Formula_.IsEmpty();
}

int TTimeFormula::Size() const
{
    return Formula_.Size();
}

size_t TTimeFormula::GetHash() const
{
    return Formula_.GetHash();
}

TString TTimeFormula::GetFormula() const
{
    return Formula_.GetFormula();
}

bool TTimeFormula::IsSatisfiedBy(TInstant time) const
{
    struct tm tm;
    time.LocalTime(&tm);
    return Formula_.Eval({
            {"hours", tm.tm_hour},
            {"minutes", tm.tm_min}}) != 0;
}

TTimeFormula::TTimeFormula(TArithmeticFormula&& arithmeticFormula)
    : Formula_(std::move(arithmeticFormula))
{ }

TTimeFormula MakeTimeFormula(const TString& formula)
{
    const static THashSet<TString> allowedVariables{"minutes", "hours"};

    auto arithmeticFormula = MakeArithmeticFormula(formula);

    for (const auto& variable : arithmeticFormula.GetVariables()) {
        if (!allowedVariables.has(variable)) {
            THROW_ERROR_EXCEPTION("Invalid variable in time formula (Variable: %Qv, TimeFormula: %Qv)",
                variable,
                formula);
        }
    }
    return TTimeFormula{std::move(arithmeticFormula)};
}

void Serialize(const TTimeFormula& timeFormula, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(timeFormula.GetFormula());
}

void Deserialize(TTimeFormula& timeFormula, NYTree::INodePtr node)
{
    timeFormula = MakeTimeFormula(node->AsString()->GetValue());
}

void TTimeFormula::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, Formula_);
}

void TTimeFormula::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    Formula_ = Load<TArithmeticFormula>(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
