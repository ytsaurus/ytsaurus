#include "db_schema.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TDBField DummyField{"dummy", NTableClient::EValueType::Boolean};
const TDBField FinalizationStartTimeField{"meta.finalization_start_time", NTableClient::EValueType::Uint64};
const TDBField FinalizersField{"meta.finalizers", NTableClient::EValueType::Any};

////////////////////////////////////////////////////////////////////////////////

TDBTable::TDBTable(TStringBuf name)
    : Name_(name)
{ }

TDBTable::~TDBTable()
{ }

void TDBTable::Initialize(
    std::initializer_list<const TDBField*> keyFields,
    std::initializer_list<const TDBField*> otherFields)
{
    KeyFields_ = std::vector<const TDBField*>(keyFields);
    std::erase(KeyFields_, nullptr);

    YT_VERIFY(NonEvaluatedKeyFields_.empty());

    for (auto* keyField : KeyFields_) {
        if (!keyField->Evaluated) {
            NonEvaluatedKeyFields_.push_back(keyField);
        }
        YT_VERIFY(!NameToField_.contains(keyField->Name));
        NameToField_[keyField->Name] = keyField;
    }

    YT_VERIFY(NonEvaluatedKeyFields_.size() > 0);

    for (auto* field : otherFields) {
        if (field) {
            YT_VERIFY(!NameToField_.contains(field->Name));
            NameToField_[field->Name] = field;
        }
    }
}

const TString& TDBTable::GetName() const
{
    return Name_;
}

const std::vector<const TDBField*>& TDBTable::GetKeyFields(bool filterEvaluated) const
{
    return filterEvaluated
        ? NonEvaluatedKeyFields_
        : KeyFields_;
}

const TDBField* TDBTable::GetField(const TStringBuf fieldName) const
{
    auto it = NameToField_.find(fieldName);
    if (it == NameToField_.end()) {
        return nullptr;
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TDBField& field,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Name: %v, Type: %v, Secure: %v, Evaluated: %v}",
        field.Name,
        field.Type,
        field.Secure,
        field.Evaluated);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TDBField* field,
    TStringBuf format)
{
    if (field) {
        FormatValue(builder, *field, format);
    } else {
        builder->AppendString("<null>");
    }
}

////////////////////////////////////////////////////////////////////////////////

const TObjectTableBase ObjectsTable;
const TWatchLogSchema WatchLogSchema;

const TTombstonesTable TombstonesTable;
const TPendingRemovalsTable PendingRemovalsTable;
const TAnnotationsTable AnnotationsTable;
const TSubjectToTypeTable SubjectToTypeTable;

////////////////////////////////////////////////////////////////////////////////

const std::vector<const TDBTable*> Tables = {
    &SubjectToTypeTable,
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
