#include <yt_udf_cpp.h>

#include <functional>

class RE2;

extern "C" RE2* RegexCreate(TUnversionedValue*);
extern "C" void RegexDestroy(RE2*);
extern "C" bool RegexFullMatch(RE2*, TUnversionedValue*);
extern "C" bool RegexPartialMatch(RE2*, TUnversionedValue*);
extern "C" void RegexReplaceFirst(TExecutionContext*, RE2*, TUnversionedValue*, TUnversionedValue*, TUnversionedValue*);
extern "C" void RegexReplaceAll(TExecutionContext*, RE2*, TUnversionedValue*, TUnversionedValue*, TUnversionedValue*);
extern "C" void RegexExtract(TExecutionContext*, RE2*, TUnversionedValue*, TUnversionedValue*, TUnversionedValue*);
extern "C" void RegexEscape(TExecutionContext*, TUnversionedValue*, TUnversionedValue*);

struct TData
{
    TData(TUnversionedValue* regexp)
        : Re2(RegexCreate(regexp))
    { }

    ~TData()
    {
        RegexDestroy(Re2);
    }

    RE2* Re2;
};

void regex_work(
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* regexp,
    std::function<void(TData*)> doWork)
{
    if (!functonContext->IsArgLiteral(0)) {
        auto data = TData(regexp);
        doWork(&data);
    } else {
        void* data = functonContext->GetPrivateData();
        if (!data) {
            data = functonContext->CreateObject<TData>(regexp);
            functonContext->SetPrivateData(data);
        }
        doWork(static_cast<TData*>(data));
    }
}

extern "C" void regex_full_match(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* result,
    TUnversionedValue* regexp,
    TUnversionedValue* input)
{
    if (regexp->Type == Null || input->Type == Null) {
        result->Type = Boolean;
        result->Data.Boolean = false;
    } else {
        regex_work(
            functonContext,
            regexp,
            [=] (TData* data) {
                result->Type = Boolean;
                result->Data.Boolean = RegexFullMatch(data->Re2, input);
            });
    }
}

extern "C" void regex_partial_match(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* result,
    TUnversionedValue* regexp,
    TUnversionedValue* input)
{
    if (regexp->Type == Null || input->Type == Null) {
        result->Type = Boolean;
        result->Data.Boolean = false;
    } else {
        regex_work(
            functonContext,
            regexp,
            [=] (TData* data) {
                result->Type = Boolean;
                result->Data.Boolean = RegexPartialMatch(data->Re2, input);
            });
    }
}

extern "C" void regex_replace_first(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* result,
    TUnversionedValue* regexp,
    TUnversionedValue* input,
    TUnversionedValue* rewrite)
{
    if (regexp->Type == Null || input->Type == Null || rewrite->Type == Null) {
        result->Type = Null;
    } else {
        regex_work(
            functonContext,
            regexp,
            [=] (TData* data) {
                RegexReplaceFirst(executionContext, data->Re2, input, rewrite, result);
            });
    }
}

extern "C" void regex_replace_all(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* result,
    TUnversionedValue* regexp,
    TUnversionedValue* input,
    TUnversionedValue* rewrite)
{
    if (regexp->Type == Null || input->Type == Null || rewrite->Type == Null) {
        result->Type = Null;
    } else {
        regex_work(
            functonContext,
            regexp,
            [=] (TData* data) {
                RegexReplaceAll(executionContext, data->Re2, input, rewrite, result);
            });
    }
}

extern "C" void regex_extract(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* result,
    TUnversionedValue* regexp,
    TUnversionedValue* input,
    TUnversionedValue* rewrite)
{
    if (regexp->Type == Null || input->Type == Null || rewrite->Type == Null) {
        result->Type = Null;
    } else {
        regex_work(
            functonContext,
            regexp,
            [=] (TData* data) {
                RegexExtract(executionContext, data->Re2, input, rewrite, result);
            });
    }
}

extern "C" void regex_escape(
    TExecutionContext* executionContext,
    NYT::NQueryClient::TFunctionContext* functonContext,
    TUnversionedValue* result,
    TUnversionedValue* input)
{
    if (input->Type == Null) {
        result->Type = Null;
    } else {
        RegexEscape(executionContext, input, result);
    }
}

