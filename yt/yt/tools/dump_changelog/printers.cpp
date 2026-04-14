#include "printers.h"

#include <yt/yt/ytlib/hive/proto/hive_service.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/memory/ref.h>

#include <util/generic/hash_set.h>

#include <google/protobuf/text_format.h>

namespace NYT::NTools::NDumpChangelog {

////////////////////////////////////////////////////////////////////////////////

void RegisterCustomPrinter(
    google::protobuf::TextFormat::Printer* printer,
    const google::protobuf::Descriptor* desc)
{
    THashSet<const google::protobuf::Descriptor*> registered;

    auto doRegister = [&] (const google::protobuf::Descriptor* desc, auto self) {
        if (!registered.insert(desc).second) {
            return;
        }

        for (int i = 0; i < desc->field_count(); ++i) {
            const auto* field = desc->field(i);

            std::unique_ptr<TCustomPrinter> holder{new TCustomPrinter()};
            if (printer->RegisterFieldValuePrinter(field, holder.get())) {
                Y_UNUSED(holder.release());
            }

            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                self(field->message_type(), self);
            }
        }
    };

    doRegister(desc, doRegister);
}

////////////////////////////////////////////////////////////////////////////////

void TCustomPrinter::PrintString(
    const TString& string,
    google::protobuf::TextFormat::BaseTextGenerator* generator) const
{
    if (!TruncateStrings_ || std::ssize(string) < TruncateLimit_) {
        FastFieldValuePrinter::PrintString(string, generator);
    } else {
        FastFieldValuePrinter::PrintString(
            string.substr(0, TruncateLimit_ / 2) + " ... " + string.substr(string.size() - TruncateLimit_ / 2),
            generator);
    }
}

void TCustomPrinter::PrintMessageStart(
    const google::protobuf::Message& message,
    int fieldIndex,
    int fieldCount,
    bool singleLineMode,
    google::protobuf::TextFormat::BaseTextGenerator* generator) const
{
    if (message.GetDescriptor()->full_name() == "NYT.NProto.TGuid") {
        generator->PrintString(": ");
    } else {
        FastFieldValuePrinter::PrintMessageStart(message, fieldIndex, fieldCount, singleLineMode, generator);
    }
}

void TCustomPrinter::PrintMessageEnd(
    const google::protobuf::Message& message,
    int fieldIndex,
    int fieldCount,
    bool singleLineMode,
    google::protobuf::TextFormat::BaseTextGenerator* generator) const
{
    if (message.GetDescriptor()->full_name() == "NYT.NProto.TGuid") {
        generator->PrintString("\n");
    } else {
        FastFieldValuePrinter::PrintMessageEnd(message, fieldIndex, fieldCount, singleLineMode, generator);
    }
}

bool TCustomPrinter::PrintMessageContent(
    const google::protobuf::Message& message,
    int /*fieldIndex*/,
    int /*fieldCount*/,
    bool /*singleLineMode*/,
    google::protobuf::TextFormat::BaseTextGenerator* generator) const
{
    if (message.GetDescriptor()->full_name() == "NYT.NProto.TGuid") {
        const auto& typedMessage = static_cast<const NYT::NProto::TGuid&>(message);
        generator->PrintString(ToString(FromProto<TGuid>(typedMessage)));
        return true;
    } else {
        return false;
    }
}

void TCustomPrinter::SetTruncateStrings(bool value)
{
    TruncateStrings_ = value;
}

void TCustomPrinter::SetTruncateLimit(int limit)
{
    TruncateLimit_ = limit;
}

bool TCustomPrinter::TruncateStrings_ = true;
int TCustomPrinter::TruncateLimit_ = 30;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<google::protobuf::Message> DeserializeMessage(
    TString type,
    TSharedRef data,
    ::google::protobuf::TextFormat::Printer* printer)
{
    const auto* descriptor = google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(type);

    if (!descriptor) {
        return nullptr;
    }

    RegisterCustomPrinter(printer, descriptor);

    const auto* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    std::unique_ptr<google::protobuf::Message> message(prototype->New());
    DeserializeProtoWithEnvelope(message.get(), data);

    return message;
}

////////////////////////////////////////////////////////////////////////////////

std::string PrintMutationContent(TString type, TSharedRef data, int indent)
{
    google::protobuf::TextFormat::Printer printer;
    printer.SetInitialIndentLevel(indent);

    auto* hiveMessagePrinter = new THiveMessagePrinter(&printer);
    printer.RegisterMessagePrinter(
        NYT::NHiveClient::NProto::TReqPostMessages::GetDescriptor(),
        hiveMessagePrinter);

    if (auto message = DeserializeMessage(type, data, &printer)) {
        TString debugString;
        printer.PrintToString(*message, &debugString);
        return debugString;
    } else {
        return std::string(indent * 2, ' ') + "<Unknown protobuf type>\n";
    }
}

////////////////////////////////////////////////////////////////////////////////

THiveMessagePrinter::THiveMessagePrinter(google::protobuf::TextFormat::Printer* printer)
    : Printer_(printer)
{ }

void THiveMessagePrinter::Print(
    const google::protobuf::Message& message,
    bool /*singleLineMode*/,
    google::protobuf::TextFormat::BaseTextGenerator* generator) const
{
    auto typedMessage = static_cast<const NYT::NHiveClient::NProto::TReqPostMessages&>(message);

    auto copy = typedMessage;
    copy.clear_messages();
    Printer_->PrintMessage(copy, generator);

    for (const auto& message : typedMessage.messages()) {
        generator->PrintString(Format("Message: %v\n", message.type()));
        generator->Indent();

        generator->PrintString(Format("Reign: %v\n", message.reign()));

        if (auto parsedMessage = DeserializeMessage(
            message.type(),
            TSharedRef::FromString(message.data()),
            Printer_))
        {
            Printer_->PrintMessage(*parsedMessage, generator);
        } else {
            generator->PrintString("<Unknown protobuf type>\n");
        }

        generator->Outdent();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools::NDumpChangelog
