#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <google/protobuf/text_format.h>

namespace NYT::NTools::NDumpChangelog {

////////////////////////////////////////////////////////////////////////////////

class TCustomPrinter
    : public google::protobuf::TextFormat::FastFieldValuePrinter
{
public:
    using TBase = FastFieldValuePrinter;

    void PrintString(
        const TString& string,
        google::protobuf::TextFormat::BaseTextGenerator* generator) const override;

    void PrintMessageStart(
        const google::protobuf::Message& message,
        int fieldIndex,
        int fieldCount,
        bool singleLineMode,
        google::protobuf::TextFormat::BaseTextGenerator* generator) const override;

    void PrintMessageEnd(
        const google::protobuf::Message& message,
        int fieldIndex,
        int fieldCount,
        bool singleLineMode,
        google::protobuf::TextFormat::BaseTextGenerator* generator) const override;

    bool PrintMessageContent(
        const google::protobuf::Message& message,
        int fieldIndex,
        int fieldCount,
        bool singleLineMode,
        google::protobuf::TextFormat::BaseTextGenerator* generator) const override;

    static void SetTruncateStrings(bool value);
    static void SetTruncateLimit(int limit);

private:
    static bool TruncateStrings_;
    static int TruncateLimit_;
};

////////////////////////////////////////////////////////////////////////////////

class THiveMessagePrinter
    : public google::protobuf::TextFormat::MessagePrinter
{
public:
    explicit THiveMessagePrinter(google::protobuf::TextFormat::Printer* printer);

    void Print(
        const google::protobuf::Message& message,
        bool singleLineMode,
        google::protobuf::TextFormat::BaseTextGenerator* generator) const override;

private:
    google::protobuf::TextFormat::Printer* Printer_;
};

////////////////////////////////////////////////////////////////////////////////

std::string PrintMutationContent(TString type, TSharedRef data, int indent = 1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NDumpChangelog
