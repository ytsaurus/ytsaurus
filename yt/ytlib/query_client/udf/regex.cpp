#include "yt_udf_cpp.h"

#include <functional>

namespace google { namespace re2 {
    class RE2;
} }

extern "C" google::re2::RE2* RegexCreate(TUnversionedValue*);
extern "C" void RegexDestroy(google::re2::RE2*);
extern "C" bool RegexFullMatch(google::re2::RE2*, TUnversionedValue*);
extern "C" bool RegexPartialMatch(google::re2::RE2*, TUnversionedValue*);
extern "C" void RegexReplaceFirst(TExecutionContext*, google::re2::RE2*, TUnversionedValue*, TUnversionedValue*, TUnversionedValue*);
extern "C" void RegexReplaceAll(TExecutionContext*, google::re2::RE2*, TUnversionedValue*, TUnversionedValue*, TUnversionedValue*);
extern "C" void RegexExtract(TExecutionContext*, google::re2::RE2*, TUnversionedValue*, TUnversionedValue*, TUnversionedValue*);
extern "C" void RegexEscape(TExecutionContext*, TUnversionedValue*, TUnversionedValue*);

struct TRe2Regex
{
    explicit TRe2Regex(TUnversionedValue* pattern)
        : Re2(RegexCreate(pattern))
    { }

    ~TRe2Regex()
    {
        RegexDestroy(Re2);
    }

    TRe2Regex(const TRe2Regex& other) = delete;
    TRe2Regex& operator= (const TRe2Regex& other) = delete;

    google::re2::RE2* Re2;
};

static void regex_apply(
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* pattern,
    std::function<void(TRe2Regex*)> func)
{
    if (!functionContext->IsLiteralArg(0)) {
        TRe2Regex regex{pattern};
        func(&regex);
    } else {
        void* regex = functionContext->GetPrivateData();
        if (!regex) {
            regex = functionContext->CreateObject<TRe2Regex>(pattern);
            if (!regex) {
                ThrowException("Failed to precompile regular expression");
            } else {
                functionContext->SetPrivateData(regex);
            }
        }
        func(static_cast<TRe2Regex*>(regex));
    }
}

extern "C" void regex_full_match(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* pattern,
    TUnversionedValue* input)
{
    if (pattern->Type == EValueType::Null || input->Type == EValueType::Null) {
        result->Type = EValueType::Boolean;
        result->Data.Boolean = false;
    } else {
        regex_apply(
            functionContext,
            pattern,
            [=] (TRe2Regex* regex) {
                result->Type = EValueType::Boolean;
                result->Data.Boolean = RegexFullMatch(regex->Re2, input);
            });
    }
}

extern "C" void regex_partial_match(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* pattern,
    TUnversionedValue* input)
{
    if (pattern->Type == EValueType::Null || input->Type == EValueType::Null) {
        result->Type = EValueType::Boolean;
        result->Data.Boolean = false;
    } else {
        regex_apply(
            functionContext,
            pattern,
            [=] (TRe2Regex* regex) {
                result->Type = EValueType::Boolean;
                result->Data.Boolean = RegexPartialMatch(regex->Re2, input);
            });
    }
}

extern "C" void regex_replace_first(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* pattern,
    TUnversionedValue* input,
    TUnversionedValue* rewrite)
{
    if (pattern->Type == EValueType::Null || input->Type == EValueType::Null || rewrite->Type == EValueType::Null) {
        result->Type = EValueType::Null;
    } else {
        regex_apply(
            functionContext,
            pattern,
            [=] (TRe2Regex* regex) {
                RegexReplaceFirst(executionContext, regex->Re2, input, rewrite, result);
            });
    }
}

extern "C" void regex_replace_all(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* pattern,
    TUnversionedValue* input,
    TUnversionedValue* rewrite)
{
    if (pattern->Type == EValueType::Null || input->Type == EValueType::Null || rewrite->Type == EValueType::Null) {
        result->Type = EValueType::Null;
    } else {
        regex_apply(
            functionContext,
            pattern,
            [=] (TRe2Regex* regex) {
                RegexReplaceAll(executionContext, regex->Re2, input, rewrite, result);
            });
    }
}

extern "C" void regex_extract(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* pattern,
    TUnversionedValue* input,
    TUnversionedValue* rewrite)
{
    if (pattern->Type == EValueType::Null || input->Type == EValueType::Null || rewrite->Type == EValueType::Null) {
        result->Type = EValueType::Null;
    } else {
        regex_apply(
            functionContext,
            pattern,
            [=] (TRe2Regex* regex) {
                RegexExtract(executionContext, regex->Re2, input, rewrite, result);
            });
    }
}

extern "C" void regex_escape(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* input)
{
    if (input->Type == EValueType::Null) {
        result->Type = EValueType::Null;
    } else {
        RegexEscape(executionContext, input, result);
    }
}

