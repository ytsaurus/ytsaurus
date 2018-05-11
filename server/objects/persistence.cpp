#include "persistence.h"
#include "object.h"
#include "type_handler.h"
#include "db_schema.h"
#include "private.h"

#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/query_client/ast.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/cast.h>
#include <yt/core/misc/collection_helpers.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

#include <array>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NNet;

using namespace google::protobuf;
using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

static const TDbField DummyField{"dummy"};

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, const TGuid& value, const TRowBufferPtr& rowBuffer, int id)
{
    auto strValue = ToString(value);
    *dbValue = value
        ? rowBuffer->Capture(MakeUnversionedStringValue(strValue, id))
        : MakeUnversionedSentinelValue(EValueType::Null);
}

void FromDbValue(TGuid* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type == EValueType::Null) {
        *value = TGuid();
        return;
    }
    if (dbValue.Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Cannot parse object id value from %Qlv",
            dbValue.Type);
    }
    *value = TGuid::FromString(TStringBuf(dbValue.Data.String, dbValue.Length));
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, const TString& value, const TRowBufferPtr& rowBuffer, int id)
{
    *dbValue = rowBuffer->Capture(MakeUnversionedStringValue(value, id));
}

void FromDbValue(TString* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type == EValueType::Null) {
        *value = TString();
        return;
    }
    if (dbValue.Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Cannot parse string value from %Qlv",
            dbValue.Type);
    }
    *value = TString(dbValue.Data.String, dbValue.Length);
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, bool value, const TRowBufferPtr& rowBuffer, int id)
{
    *dbValue = rowBuffer->Capture(MakeUnversionedBooleanValue(value, id));
}

void FromDbValue(bool* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type == EValueType::Null) {
        *value = false;
        return;
    }
    if (dbValue.Type != EValueType::Boolean) {
        THROW_ERROR_EXCEPTION("Cannot parse bool value from %Qlv",
            dbValue.Type);
    }
    *value = dbValue.Data.Boolean;
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, const TYsonString& value, const TRowBufferPtr& rowBuffer, int id)
{
    Y_ASSERT(value.GetType() == EYsonType::Node);
    *dbValue = rowBuffer->Capture(MakeUnversionedAnyValue(value.GetData(), id));
}

void FromDbValue(TYsonString* value, const TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse YSON string value from %Qlv",
            dbValue.Type);
    }
    *value = TYsonString(TString(dbValue.Data.String, dbValue.Length));
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, i64 value, const TRowBufferPtr& /*rowBuffer*/, int id)
{
    *dbValue = MakeUnversionedInt64Value(value, id);
}

void FromDbValue(i64* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Int64) {
        THROW_ERROR_EXCEPTION("Cannot parse int64 value from %Qlv",
            dbValue.Type);
    }
    *value = dbValue.Data.Int64;
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, ui64 value, const TRowBufferPtr& /*rowBuffer*/, int id)
{
    *dbValue = MakeUnversionedUint64Value(value, id);
}

void FromDbValue(ui64* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Uint64) {
        THROW_ERROR_EXCEPTION("Cannot parse uint64 value from %Qlv",
            dbValue.Type);
    }
    *value = dbValue.Data.Uint64;
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, ui32 value, const TRowBufferPtr& /*rowBuffer*/, int id)
{
    *dbValue = MakeUnversionedUint64Value(value, id);
}

void FromDbValue(ui32* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Uint64) {
        THROW_ERROR_EXCEPTION("Cannot parse uint32 value from %Qlv",
            dbValue.Type);
    }
    *value = CheckedIntegralCast<ui32>(dbValue.Data.Uint64);
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, ui16 value, const TRowBufferPtr& /*rowBuffer*/, int id)
{
    *dbValue = MakeUnversionedUint64Value(value, id);
}

void FromDbValue(ui16* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Uint64) {
        THROW_ERROR_EXCEPTION("Cannot parse uint16 value from %Qlv",
            dbValue.Type);
    }
    *value = CheckedIntegralCast<ui16>(dbValue.Data.Uint64);
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, double value, const TRowBufferPtr& /*rowBuffer*/, int id)
{
    *dbValue = MakeUnversionedDoubleValue(value, id);
}

void FromDbValue(double* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Double) {
        THROW_ERROR_EXCEPTION("Cannot parse double value from %Qlv",
            dbValue.Type);
    }
    *value = dbValue.Data.Double;
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, TInstant value, const TRowBufferPtr& /*rowBuffer*/, int id)
{
    *dbValue = MakeUnversionedUint64Value(value.MicroSeconds(), id);
}

void FromDbValue(TInstant* value, const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Uint64) {
        THROW_ERROR_EXCEPTION("Cannot parse instant from %Qlv",
            dbValue.Type);
    }
    *value = TInstant::MicroSeconds(dbValue.Data.Uint64);
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, const IMapNodePtr& value, const TRowBufferPtr& rowBuffer, int id)
{
    *dbValue = rowBuffer->Capture(MakeUnversionedAnyValue(ConvertToYsonString(value).GetData(), id));
}

void FromDbValue(IMapNodePtr* value, const TUnversionedValue& dbValue)
{
    if (dbValue.Type == EValueType::Null) {
        *value = nullptr;
    }
    if (dbValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse YSON map from %Qlv",
            dbValue.Type);
    }
    *value = ConvertTo<IMapNodePtr>(TYsonString(dbValue.Data.String, dbValue.Length));
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValue(TUnversionedValue* dbValue, const TIP6Address& value, const TRowBufferPtr& rowBuffer, int id)
{
    ToDbValue(dbValue, ToString(value), rowBuffer, id);
}

void FromDbValue(TIP6Address* value, const TUnversionedValue& dbValue)
{
    if (dbValue.Type == EValueType::Null) {
        *value = TIP6Address();
    }
    auto strValue = FromDbValue<TString>(dbValue);
    *value = TIP6Address::FromString(strValue);
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValueImpl(
    TUnversionedValue* dbValue,
    const Message& value,
    const TProtobufMessageType* type,
    const TRowBufferPtr& rowBuffer,
    int id)
{
    auto byteSize = value.ByteSize();
    auto* pool = rowBuffer->GetPool();
    auto* wireBuffer = pool->AllocateUnaligned(byteSize);
    YCHECK(value.SerializePartialToArray(wireBuffer, byteSize));
    ArrayInputStream inputStream(wireBuffer, byteSize);
    TString ysonBytes;
    TStringOutput outputStream(ysonBytes);
    TYsonWriter ysonWriter(&outputStream);
    ParseProtobuf(&ysonWriter, &inputStream, type);
    *dbValue = rowBuffer->Capture(MakeUnversionedAnyValue(ysonBytes, id));
}

void FromDbValueImpl(
    Message* value,
    const TProtobufMessageType* type,
    const TUnversionedValue& dbValue)
{
    if (dbValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse a protobuf message from %Qlv",
            dbValue.Type);
    }
    TString wireBytes;
    StringOutputStream outputStream(&wireBytes);
    auto protobufWriter = CreateProtobufWriter(&outputStream, type);
    ParseYsonStringBuffer(
        TStringBuf(dbValue.Data.String, dbValue.Length),
        EYsonType::Node,
        protobufWriter.get());
    if (!value->ParseFromArray(wireBytes.data(), wireBytes.size())) {
        THROW_ERROR_EXCEPTION("Error parsing %v from wire bytes",
            value->GetTypeName());
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValueImpl(
    TUnversionedValue* dbValue,
    const std::function<bool(TUnversionedValue*)> producer,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id)
{
    TString ysonBytes;
    TStringOutput outputStream(ysonBytes);
    NYT::NYson::TYsonWriter writer(&outputStream);
    writer.OnBeginList();

    int count = 0;
    while (true) {
        writer.OnListItem();
        TUnversionedValue itemValue;
        if (!producer(&itemValue)) {
            break;
        }
        DbValueToYson(itemValue, &writer);
        ++count;
    }
    writer.OnEndList();

    if (count == 0) {
        *dbValue = NYT::NTableClient::MakeUnversionedSentinelValue(NYT::NTableClient::EValueType::Null, id);
    } else {
        *dbValue = rowBuffer->Capture(NYT::NTableClient::MakeUnversionedAnyValue(ysonBytes, id));
    }
}

void FromDbValueImpl(
    std::function<google::protobuf::Message*()> appender,
    const TProtobufMessageType* type,
    const TUnversionedValue& dbValue)
{
    if (dbValue.Type == EValueType::Null) {
        return;
    }

    if (dbValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse vector from %Qlv",
            dbValue.Type);
    }

    class TConsumer
        : public IYsonConsumer
    {
    public:
        TConsumer(
            std::function<google::protobuf::Message*()> appender,
            const TProtobufMessageType* type)
            : Appender_(std::move(appender))
            , Type_(type)
            , OutputStream_(&WireBytes_)
        { }

        virtual void OnStringScalar(TStringBuf value) override
        {
            GetUnderlying()->OnStringScalar(value);
        }

        virtual void OnInt64Scalar(i64 value) override
        {
            GetUnderlying()->OnInt64Scalar(value);
        }

        virtual void OnUint64Scalar(ui64 value) override
        {
            GetUnderlying()->OnUint64Scalar(value);
        }

        virtual void OnDoubleScalar(double value) override
        {
            GetUnderlying()->OnDoubleScalar(value);
        }

        virtual void OnBooleanScalar(bool value) override
        {
            GetUnderlying()->OnBooleanScalar(value);
        }

        virtual void OnEntity() override
        {
            GetUnderlying()->OnEntity();
        }

        virtual void OnBeginList() override
        {
            if (Depth_ > 0) {
                GetUnderlying()->OnBeginList();
            }
            ++Depth_;
        }

        virtual void OnListItem() override
        {
            if (Depth_ == 1) {
                NextElement();
            } else {
                GetUnderlying()->OnListItem();
            }
        }

        virtual void OnEndList() override
        {
            --Depth_;
            if (Depth_ == 0) {
                FlushElement();
            }
        }

        virtual void OnBeginMap() override
        {
            ++Depth_;
            GetUnderlying()->OnBeginMap();
        }

        virtual void OnKeyedItem(TStringBuf key) override
        {
            GetUnderlying()->OnKeyedItem(key);
        }

        virtual void OnEndMap() override
        {
            --Depth_;
            GetUnderlying()->OnEndMap();
        }

        virtual void OnBeginAttributes() override
        {
            GetUnderlying()->OnBeginAttributes();
        }

        virtual void OnEndAttributes() override
        {
            GetUnderlying()->OnEndAttributes();
        }

        virtual void OnRaw(TStringBuf yson, EYsonType type) override
        {
            GetUnderlying()->OnRaw(yson, type);
        }

    private:
        const std::function<google::protobuf::Message*()> Appender_;
        const TProtobufMessageType* const Type_;

        std::unique_ptr<IYsonConsumer> Underlying_;
        int Depth_ = 0;

        TString WireBytes_;
        StringOutputStream OutputStream_;


        IYsonConsumer* GetUnderlying()
        {
            if (!Underlying_) {
                THROW_ERROR_EXCEPTION("YSON value must be a list without attributes");
            }
            return Underlying_.get();
        }

        void NextElement()
        {
            if (Underlying_) {
                FlushElement();
            }
            WireBytes_.clear();
            Underlying_ = CreateProtobufWriter(&OutputStream_, Type_);
        }

        void FlushElement()
        {
            auto* value = Appender_();
            if (!value->ParseFromArray(WireBytes_.data(), WireBytes_.size())) {
                THROW_ERROR_EXCEPTION("Error parsing %v from wire bytes",
                    value->GetTypeName());
            }
            Underlying_.reset();
        }
    } consumer(std::move(appender), type);

    ParseYsonStringBuffer(
        TStringBuf(dbValue.Data.String, dbValue.Length),
        EYsonType::Node,
        &consumer);
}

void FromDbValueImpl(
    std::function<void(const TUnversionedValue&)> appender,
    const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type == EValueType::Null) {
        return;
    }

    if (dbValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse a vector from %Qlv",
            dbValue.Type);
    }

    class TConsumer
        : public TYsonConsumerBase
    {
    public:
        explicit TConsumer(std::function<void(const TUnversionedValue&)> appender)
            : Appender_(std::move(appender))
        { }

        virtual void OnStringScalar(TStringBuf value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedStringValue(value));
        }

        virtual void OnInt64Scalar(i64 value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedInt64Value(value));
        }

        virtual void OnUint64Scalar(ui64 value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedUint64Value(value));
        }

        virtual void OnDoubleScalar(double value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedDoubleValue(value));
        }

        virtual void OnBooleanScalar(bool value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedBooleanValue(value));
        }

        virtual void OnEntity() override
        {
            THROW_ERROR_EXCEPTION("YSON entities are not supported");
        }

        virtual void OnBeginList() override
        {
            EnsureNotInList();
            InList_ = true;
        }

        virtual void OnListItem() override
        { }

        virtual void OnEndList() override
        { }

        virtual void OnBeginMap() override
        {
            THROW_ERROR_EXCEPTION("YSON maps are not supported");
        }

        virtual void OnKeyedItem(TStringBuf /*key*/) override
        {
            Y_UNREACHABLE();
        }

        virtual void OnEndMap() override
        {
            Y_UNREACHABLE();
        }

        virtual void OnBeginAttributes() override
        {
            THROW_ERROR_EXCEPTION("YSON attributes are not supported");
        }

        virtual void OnEndAttributes() override
        {
            Y_UNREACHABLE();
        }

    private:
        const std::function<void(const TUnversionedValue&)> Appender_;

        bool InList_ = false;

        void EnsureInList()
        {
            if (!InList_) {
                THROW_ERROR_EXCEPTION("YSON list expected");
            }
        }

        void EnsureNotInList()
        {
            if (InList_) {
                THROW_ERROR_EXCEPTION("YSON list is unexpected");
            }
        }
    } consumer(std::move(appender));

    ParseYsonStringBuffer(
        TStringBuf(dbValue.Data.String, dbValue.Length),
        EYsonType::Node,
        &consumer);
}

////////////////////////////////////////////////////////////////////////////////

void DbValueToYson(const TUnversionedValue& dbValue, IYsonConsumer* consumer)
{
    switch (dbValue.Type) {
        case EValueType::Int64:
            consumer->OnInt64Scalar(dbValue.Data.Int64);
            break;
        case EValueType::Uint64:
            consumer->OnUint64Scalar(dbValue.Data.Uint64);
            break;
        case EValueType::Double:
            consumer->OnDoubleScalar(dbValue.Data.Double);
            break;
        case EValueType::String:
            consumer->OnStringScalar(TStringBuf(dbValue.Data.String, dbValue.Length));
            break;
        case EValueType::Any:
            consumer->OnRaw(TStringBuf(dbValue.Data.String, dbValue.Length), EYsonType::Node);
            break;
        case EValueType::Boolean:
            consumer->OnBooleanScalar(dbValue.Data.Boolean);
            break;
        case EValueType::Null:
            consumer->OnEntity();
            break;
        default:
            Y_UNREACHABLE();
    }
}

TYsonString DbValueToYson(const TUnversionedValue& dbValue)
{
    TString data;
    data.reserve(GetYsonSize(dbValue));
    TStringOutput output(data);
    TYsonWriter writer(&output, EYsonFormat::Binary);
    DbValueToYson(dbValue, &writer);
    return TYsonString(std::move(data));
}

TRange<TUnversionedValue> CaptureCompositeObjectKey(
    const TObject* object,
    const TRowBufferPtr& rowBuffer)
{
    auto* typeHandler = object->GetTypeHandler();
    auto parentType = typeHandler->GetParentType();
    auto capture = [&] (const auto& key) {
        auto* values = reinterpret_cast<TUnversionedValue*>(rowBuffer->GetPool()->AllocateAligned(key.size() * sizeof (TUnversionedValue)));
        ::memcpy(values, key.data(), sizeof (TUnversionedValue) * key.size());
        return MakeRange(values, key.size());
    };
    if (parentType == EObjectType::Null) {
        return capture(
            ToDbValues(
                rowBuffer,
                object->GetId()));
    } else {
        return capture(
            ToDbValues(
                rowBuffer,
                object->GetParentId(),
                object->GetId()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TObjectExistenceChecker::TObjectExistenceChecker(TObject* object)
    : Object_(object)
{ }

TObject* TObjectExistenceChecker::GetObject() const
{
    return Object_;
}

void TObjectExistenceChecker::ScheduleCheck() const
{
    auto* this_ = const_cast<TObjectExistenceChecker*>(this);
    if (this_->CheckScheduled_) {
        return;
    }
    this_->CheckScheduled_ = true;
    Object_->GetSession()->ScheduleLoad(
        [=] (ILoadContext* context) {
            this_->LoadFromDb(context);
        });
}

bool TObjectExistenceChecker::Check() const
{
    if (!Checked_) {
        ScheduleCheck();
        Object_->GetSession()->FlushLoads();
    }
    Y_ASSERT(Checked_);
    return Exists_;
}

void TObjectExistenceChecker::LoadFromDb(ILoadContext* context)
{
    Y_ASSERT(!Checked_);

    auto lookupHandler = [&] (const TNullable<TRange<NYT::NTableClient::TVersionedValue>>& maybeValues) {
        Y_ASSERT(!Checked_);
        Checked_ = true;
        auto* typeHandler = Object_->GetTypeHandler();
        if (typeHandler->GetParentType() == EObjectType::Null) {
            Exists_ = maybeValues.HasValue() && (*maybeValues)[1].Type == EValueType::Null;
        } else {
            Exists_ = maybeValues.HasValue();
        }
    };

    auto* typeHandler = Object_->GetTypeHandler();
    if (typeHandler->GetParentType() == EObjectType::Null) {
        const auto* table = typeHandler->GetTable();
        context->ScheduleLookup(
            table,
            ToDbValues(
                context->GetRowBuffer(),
                Object_->GetId()),
            MakeArray(
                // XXX(babenko): creation time is only needed to work around the bug in write ts
                &ObjectsTable.Fields.Meta_CreationTime,
                &ObjectsTable.Fields.Meta_RemovalTime),
            std::move(lookupHandler));
    } else {
        context->ScheduleLookup(
            &ParentsTable,
            ToDbValues(
                context->GetRowBuffer(),
                Object_->GetId(),
                Object_->GetType()),
            MakeArray(&ParentsTable.Fields.ParentId),
            std::move(lookupHandler));
    }
}

void TObjectExistenceChecker::StoreToDb(IStoreContext* /*context*/)
{
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

TAttributeBase::TAttributeBase(TObject* owner)
    : Owner_(owner)
{
    Owner_->RegisterAttribute(this);
}

TObject* TAttributeBase::GetOwner() const
{
    return Owner_;
}

void TAttributeBase::ThrowObjectMissing() const
{
    THROW_ERROR_EXCEPTION("Object %Qv of type %Qlv is missing",
        Owner_->GetId(),
        Owner_->GetType());
}

void TAttributeBase::DoScheduleLoad(int priority) const
{
    auto this_ = const_cast<TAttributeBase*>(this);
    if (this_->LoadScheduled_) {
        return;
    }
    this_->LoadScheduled_ = true;
    Owner_->GetSession()->ScheduleLoad(
        [=] (ILoadContext* context) {
            this_->LoadScheduled_ = false;
            this_->LoadFromDb(context);
        },
        priority);
}

void TAttributeBase::DoScheduleStore() const
{
    auto this_ = const_cast<TAttributeBase*>(this);
    if (this_->StoreScheduled_) {
        return;
    }
    this_->StoreScheduled_ = true;
    Owner_->GetSession()->ScheduleStore(
        [=] (IStoreContext* context) {
            this_->StoreScheduled_ = false;
            this_->StoreToDb(context);
        });
}

void TAttributeBase::LoadFromDb(ILoadContext* context)
{ }

void TAttributeBase::StoreToDb(IStoreContext* context)
{ }

void TAttributeBase::OnObjectCreated()
{ }

void TAttributeBase::OnObjectRemoved()
{ }

////////////////////////////////////////////////////////////////////////////////

TParentIdAttribute::TParentIdAttribute(TObject* owner, const TObjectId& parentId)
    : TAttributeBase(owner)
    , NeedsParentId_(Owner_->GetTypeHandler()->GetParentType() != EObjectType::Null)
    , ParentId_(parentId)
{
    if (!ParentId_ && NeedsParentId_) {
        DoScheduleLoad(ISession::ParentLoadPriority);
    }
}

const TObjectId& TParentIdAttribute::GetId() const
{
    if (!NeedsParentId_) {
        return ParentId_;
    }

    if (!ParentId_ && !Missing_) {
        Owner_->GetSession()->FlushLoads();
        Y_ASSERT(ParentId_ || Missing_);
    }

    if (Missing_) {
        ThrowObjectMissing();
    }

    return ParentId_;
}

void TParentIdAttribute::LoadFromDb(ILoadContext* context)
{
    Y_ASSERT(!ParentId_);
    Y_ASSERT(!Missing_);

    context->ScheduleLookup(
        &ParentsTable,
        ToDbValues(
            context->GetRowBuffer(),
            Owner_->GetId(),
            Owner_->GetType()),
        MakeArray(&ParentsTable.Fields.ParentId),
        [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
            if (maybeValues) {
                Y_ASSERT(maybeValues->Size() == 1);
                try {
                    FromDbValue(&ParentId_, (*maybeValues)[0]);
                    LOG_DEBUG("Object parent resolved (ObjectId: %v, ObjectType: %v, ParentId: %v)",
                        Owner_->GetId(),
                        Owner_->GetType(),
                        ParentId_);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading parent id value of for object %v of type %Qlv",
                        Owner_->GetId(),
                        Owner_->GetType())
                        << ex;
                }
            } else {
                Missing_ = true;
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

TChildrenAttributeBase::TChildrenAttributeBase(TObject* owner)
    : TAttributeBase(owner)
{ }

void TChildrenAttributeBase::ScheduleLoad() const
{
    DoScheduleLoad();
}

const THashSet<TObject*>& TChildrenAttributeBase::UntypedLoad() const
{
    ScheduleLoad();
    Owner_->GetSession()->FlushLoads();
    return *Children_;
}

void TChildrenAttributeBase::DoAdd(TObject* child)
{
    AddedChildren_.insert(child);
    RemovedChildren_.erase(child);
    if (Children_) {
        Children_->insert(child);
    }
}

void TChildrenAttributeBase::DoRemove(TObject* child)
{
    RemovedChildren_.insert(child);
    AddedChildren_.erase(child);
    if (Children_) {
        Children_->erase(child);
    }
}

void TChildrenAttributeBase::LoadFromDb(ILoadContext* context)
{
    if (Children_) {
        return;
    }

    auto childrenType = GetChildrenType();
    auto* session = Owner_->GetSession();
    auto* typeHandler = session->GetTypeHandler(childrenType);

    context->ScheduleSelect(
        Format("%v from %v where %v = %v and is_null(%v)",
            FormatId(typeHandler->GetIdField()->Name),
            FormatId(context->GetTablePath(typeHandler->GetTable())),
            FormatId(typeHandler->GetParentIdField()->Name),
            FormatLiteralValue(Owner_->GetId()),
            FormatId(ObjectsTable.Fields.Meta_RemovalTime.Name)),
        [=] (const IUnversionedRowsetPtr& rowset) {
            Y_ASSERT(!Children_);
            auto rows = rowset->GetRows();
            Children_.Emplace();
            Children_->reserve(rows.Size());
            for (auto row : rows) {
                Y_ASSERT(row.GetCount() == 1);
                auto childId = FromDbValue<TObjectId>(row[0]);
                auto* child = Owner_->GetSession()->GetObject(GetChildrenType(), childId);
                Children_->insert(child);
            }
            for (auto* object : AddedChildren_) {
                Children_->insert(object);
            }
            for (auto* object : RemovedChildren_) {
                Children_->erase(object);
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeBase::TScalarAttributeBase(TObject* owner, const TScalarAttributeSchemaBase* schema)
    : TAttributeBase(owner)
    , Schema_(schema)
{ }

void TScalarAttributeBase::ScheduleLoad() const
{
    if (LoadScheduled_ || Loaded_) {
        return;
    }

    DoScheduleLoad();
    LoadScheduled_ = true;
}

void TScalarAttributeBase::ScheduleStore()
{
    if (StoreScheduled_) {
        return;
    }

    DoScheduleStore();
    StoreScheduled_ = true;
}

void TScalarAttributeBase::OnLoad() const
{
    ScheduleLoad();

    if (LoadScheduled_) {
        Owner_->GetSession()->FlushLoads();
    }

    Owner_->ValidateExists();
    YCHECK(!Missing_);
}

void TScalarAttributeBase::OnStore()
{
    auto ownerState = Owner_->GetState();
    YCHECK(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    ScheduleStore();
}

void TScalarAttributeBase::LoadFromDb(ILoadContext* context)
{
    if (!LoadScheduled_) {
        return;
    }

    auto key = CaptureCompositeObjectKey(Owner_, context->GetRowBuffer());

    auto* typeHandler = Owner_->GetTypeHandler();
    const auto* table = typeHandler->GetTable();

    context->ScheduleLookup(
        table,
        key,
        MakeArray(Schema_->Field),
        [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
            if (maybeValues) {
                Y_ASSERT(maybeValues->Size() == 1);
                try {
                    LoadOldValue((*maybeValues)[0], context);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading value of [%v.%v] for object %Qv of type %Qlv",
                        table->Name,
                        Schema_->Field->Name,
                        Owner_->GetId(),
                        Owner_->GetType())
                        << ex;
                }
            } else {
                Missing_ = true;
            }
        });
}

void TScalarAttributeBase::StoreToDb(IStoreContext* context)
{
    auto ownerState = Owner_->GetState();
    if (ownerState == EObjectState::Removed || ownerState == EObjectState::CreatedRemoved) {
        return;
    }

    auto key = CaptureCompositeObjectKey(Owner_, context->GetRowBuffer());

    auto* typeHandler = Owner_->GetTypeHandler();
    const auto* table = typeHandler->GetTable();

    TUnversionedValue value;
    try {
        StoreNewValue(&value, context);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error storing value of [%v.%v] for object %Qv of type %Qlv",
            table->Name,
            Schema_->Field->Name,
            Owner_->GetId(),
            Owner_->GetType())
            << ex;
    }

    context->WriteRow(
        table,
        key,
        MakeArray(Schema_->Field),
        MakeArray(context->GetRowBuffer()->Capture(value)));
}

void TScalarAttributeBase::OnObjectCreated()
{
    SetDefaultValues();
    Loaded_ = true;
    OnStore();
}

////////////////////////////////////////////////////////////////////////////////

TTimestampAttribute::TTimestampAttribute(TObject* owner, const TTimestampAttributeSchema* schema)
    : TScalarAttributeBase(owner, schema)
{ }

TTimestamp TTimestampAttribute::Load() const
{
    OnLoad();
    return Timestamp_;
}

TTimestampAttribute::operator TTimestamp() const
{
    return Load();
}

void TTimestampAttribute::Touch()
{
    auto ownerState = Owner_->GetState();
    YCHECK(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    ScheduleStore();
}

void TTimestampAttribute::SetDefaultValues()
{
    Timestamp_ = NullTimestamp;
}

void TTimestampAttribute::LoadOldValue(const TVersionedValue& value, ILoadContext* /*context*/)
{
    Timestamp_ = value.Timestamp;
}

void TTimestampAttribute::StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* /*context*/)
{
    *dbValue = MakeUnversionedSentinelValue(EValueType::Null);
}

////////////////////////////////////////////////////////////////////////////////

TOneToManyAttributeBase::TOneToManyAttributeBase(
    TObject* owner,
    const TOneToManyAttributeSchemaBase* schema)
    : TAttributeBase(owner)
    , Schema_(schema)
{ }

void TOneToManyAttributeBase::ScheduleLoad() const
{
    DoScheduleLoad();
}

const THashSet<TObject*>& TOneToManyAttributeBase::UntypedLoad() const
{
    ScheduleLoad();
    Owner_->GetSession()->FlushLoads();
    return *ForeignObjects_;
}

void TOneToManyAttributeBase::DoAdd(TObject* many)
{
    AddedForeignObjects_.insert(many);
    RemovedForeignObjects_.erase(many);
    if (ForeignObjects_) {
        ForeignObjects_->insert(many);
    }
    DoScheduleStore();
}

void TOneToManyAttributeBase::DoRemove(TObject* many)
{
    RemovedForeignObjects_.insert(many);
    AddedForeignObjects_.erase(many);
    if (ForeignObjects_) {
        ForeignObjects_->erase(many);
    }
    DoScheduleStore();
}

void TOneToManyAttributeBase::LoadFromDb(ILoadContext* context)
{
    if (ForeignObjects_) {
        return;
    }

    context->ScheduleSelect(
        Format("%v from %v where %v = %v",
            FormatId(Schema_->ForeignKeyField->Name),
            FormatId(context->GetTablePath(Schema_->Table)),
            FormatId(Schema_->PrimaryKeyField->Name),
            FormatLiteralValue(Owner_->GetId())),
        [=] (const IUnversionedRowsetPtr& rowset) {
            Y_ASSERT(!ForeignObjects_);
            auto rows = rowset->GetRows();
            ForeignObjects_.Emplace();
            ForeignObjects_->reserve(rows.Size());
            for (auto row : rows) {
                Y_ASSERT(row.GetCount() == 1);
                auto foreignId = FromDbValue<TObjectId>(row[0]);
                auto* foreignObject = Owner_->GetSession()->GetObject(GetForeignObjectType(), foreignId);
                ForeignObjects_->insert(foreignObject);
            }
            for (auto* object : AddedForeignObjects_) {
                ForeignObjects_->insert(object);
            }
            for (auto* object : RemovedForeignObjects_) {
                ForeignObjects_->erase(object);
            }
        });
}

void TOneToManyAttributeBase::StoreToDb(IStoreContext* context)
{
    const auto& rowBuffer = context->GetRowBuffer();

    for (auto* object : AddedForeignObjects_) {
        context->WriteRow(
            Schema_->Table,
            ToDbValues(
                rowBuffer,
                Owner_->GetId(),
                object->GetId()),
            MakeArray(&DummyField),
            MakeArray(MakeUnversionedSentinelValue(EValueType::Null)));
    }

    for (auto* object : RemovedForeignObjects_) {
        context->DeleteRow(
            Schema_->Table,
            ToDbValues(
                rowBuffer,
                Owner_->GetId(),
                object->GetId()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TAnnotationsAttribute::TAnnotationsAttribute(TObject* owner)
    : TAttributeBase(owner)
{ }

void TAnnotationsAttribute::ScheduleLoad(const TString& key) const
{
    if (KeyToValue_.find(key) != KeyToValue_.end() || ScheduledLoadAll_ || LoadedAll_) {
        return;
    }

    ScheduledLoadKeys_.insert(key);
    DoScheduleLoad();
}

TNullable<TYsonString> TAnnotationsAttribute::Load(const TString& key) const
{
    ScheduleLoad(key);
    Owner_->GetSession()->FlushLoads();
    auto it = KeyToValue_.find(key);
    Y_ASSERT(it != KeyToValue_.end());
    return it->second;
}

void TAnnotationsAttribute::ScheduleLoadAll() const
{
    if (ScheduledLoadAll_ || LoadedAll_) {
        return;

    }

    ScheduledLoadAll_ = true;
    DoScheduleLoad();
}

std::vector<std::pair<TString, NYT::NYson::TYsonString>> TAnnotationsAttribute::LoadAll() const
{
    ScheduleLoadAll();
    Owner_->GetSession()->FlushLoads();
    std::vector<std::pair<TString, NYT::NYson::TYsonString>> result;
    result.reserve(KeyToValue_.size());
    for (const auto& pair : KeyToValue_) {
        if (pair.second) {
            result.emplace_back(pair.first, *pair.second);
        }
    }
    return result;
}

void TAnnotationsAttribute::Store(const TString& key, const TNullable<TYsonString>& value)
{
    auto ownerState = Owner_->GetState();
    YCHECK(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    KeyToValue_[key] = value;
    ScheduledStoreKeys_.insert(key);
    DoScheduleStore();
}

void TAnnotationsAttribute::LoadFromDb(ILoadContext* context)
{
    const auto& rowBuffer = context->GetRowBuffer();

    auto ownerState = Owner_->GetState();
    if (ownerState == EObjectState::Removed || ownerState == EObjectState::Removing) {
        auto primaryKey = ToDbValue(Owner_->GetId(), rowBuffer);
        Y_ASSERT(primaryKey.Type == EValueType::String);
        TString primaryKeyString(primaryKey.Data.String, primaryKey.Length);

        context->ScheduleSelect(
            Format("%v from %v where %v = %v and %v = %v",
                FormatId(AnnotationsTable.Fields.Name.Name),
                FormatId(context->GetTablePath(&AnnotationsTable)),
                FormatId(AnnotationsTable.Fields.ObjectId.Name),
                FormatLiteralValue(primaryKeyString),
                FormatId(AnnotationsTable.Fields.ObjectType.Name),
                static_cast<int>(Owner_->GetType())),
            [=] (const IUnversionedRowsetPtr& rowset) {
                auto rows = rowset->GetRows();

                KeyToValue_.clear();
                KeyToValue_.reserve(rows.Size());

                ScheduledStoreKeys_.clear();
                ScheduledStoreKeys_.reserve(rows.Size());

                for (auto row : rows) {
                    Y_ASSERT(row.GetCount() == 1);
                    auto key = FromDbValue<TString>(row[0]);
                    YCHECK(KeyToValue_.emplace(key, Null).second);
                    YCHECK(ScheduledStoreKeys_.emplace(key).second);
                }
            });
    } else {
        for (const auto& attributeKey : ScheduledLoadKeys_) {
            context->ScheduleLookup(
                &AnnotationsTable,
                ToDbValues(
                    rowBuffer,
                    Owner_->GetId(),
                    Owner_->GetType(),
                    attributeKey),
                MakeArray(&AnnotationsTable.Fields.Value),
                [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
                    if (maybeValues) {
                        Y_ASSERT(maybeValues->Size() == 1);
                        const auto& value = (*maybeValues)[0];
                        KeyToValue_[attributeKey] = FromDbValue<TYsonString>(value);
                    } else {
                        KeyToValue_[attributeKey] = Null;
                    }
                    YCHECK(ScheduledLoadKeys_.erase(attributeKey) == 1);
                });
        }

        if (ScheduledLoadAll_) {
            auto primaryKey = ToDbValue(Owner_->GetId(), rowBuffer);
            Y_ASSERT(primaryKey.Type == EValueType::String);
            TString primaryKeyString(primaryKey.Data.String, primaryKey.Length);

            context->ScheduleSelect(
                Format("%v, %v from %v where %v = %v and %v = %v",
                    FormatId(AnnotationsTable.Fields.Name.Name),
                    FormatId(AnnotationsTable.Fields.Value.Name),
                    FormatId(context->GetTablePath(&AnnotationsTable)),
                    FormatId(AnnotationsTable.Fields.ObjectId.Name),
                    FormatLiteralValue(primaryKeyString),
                    FormatId(AnnotationsTable.Fields.ObjectType.Name),
                    static_cast<int>(Owner_->GetType())),
                [=] (const IUnversionedRowsetPtr& rowset) {
                    auto rows = rowset->GetRows();
                    for (auto row : rows) {
                        Y_ASSERT(row.GetCount() == 2);
                        auto key = FromDbValue<TString>(row[0]);
                        auto value = FromDbValue<TYsonString>(row[1]);
                        KeyToValue_.emplace(std::move(key), std::move(value));
                    }
                });
        }
    }
}

void TAnnotationsAttribute::StoreToDb(IStoreContext* context)
{
    for (const auto& attributeKey : ScheduledStoreKeys_) {
        auto it = KeyToValue_.find(attributeKey);
        Y_ASSERT(it != KeyToValue_.end());
        const auto& maybeAttributeValue = it->second;

        auto keyValues = ToDbValues(
            context->GetRowBuffer(),
            Owner_->GetId(),
            Owner_->GetType(),
            attributeKey);

        if (maybeAttributeValue) {
            context->WriteRow(
                &AnnotationsTable,
                keyValues,
                MakeArray(&AnnotationsTable.Fields.Value),
                ToDbValues(
                    context->GetRowBuffer(),
                    *maybeAttributeValue));
        } else {
            context->DeleteRow(
                &AnnotationsTable,
                keyValues);
        }
    }
}

void TAnnotationsAttribute::OnObjectCreated()
{
    LoadedAll_ = true;
}

void TAnnotationsAttribute::OnObjectRemoved()
{
    DoScheduleLoad();
    DoScheduleStore();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

