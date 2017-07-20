#include "serialize.h"
#include "helpers.h"

#include <yt/core/yson/lexer_detail.h>

#include <yt/core/ytree/node.h>

#include <yt/core/misc/finally.h>

#include <numeric>


namespace NYT {

namespace {

////////////////////////////////////////////////////////////////////////////////

using NYson::TToken;
using NYson::ETokenType;
using NYson::EYsonType;
using NYson::IYsonConsumer;
using NYTree::INodePtr;
using NYTree::ENodeType;

////////////////////////////////////////////////////////////////////////////////

Py::Callable GetYsonType(const std::string& name)
{
    // TODO(ignat): Make singleton
    static Py::Object ysonTypesModule;
    if (ysonTypesModule.isNone()) {
        auto ptr = PyImport_ImportModule("yt.yson.yson_types");
        if (!ptr) {
            throw Py::RuntimeError("Failed to import module yt.yson.yson_types");
        }
        ysonTypesModule = ptr;
    }
    return Py::Callable(GetAttr(ysonTypesModule, name));
}

Py::Exception CreateYsonError(const std::string& message, TContext* context)
{
    static Py::Callable ysonErrorClass;
    if (ysonErrorClass.isNone()) {
        auto ysonModule = Py::Module(PyImport_ImportModule("yt.yson.common"), true);
        ysonErrorClass = Py::Callable(GetAttr(ysonModule, "YsonError"));
    }
    Py::Dict attributes;
    if (context->RowIndex) {
        attributes.setItem("row_index", Py::Long(context->RowIndex.Get()));
    }

    bool endedWithDelimiter = false;
    TStringBuilder builder;
    for (const auto& pathPart : context->PathParts) {
        if (pathPart.InAttributes) {
            YCHECK(!endedWithDelimiter);
            builder.AppendString("/@");
            endedWithDelimiter = true;
        } else {
            if (!endedWithDelimiter) {
                builder.AppendChar('/');
            }
            if (!pathPart.Key.empty()) {
                builder.AppendString(pathPart.Key);
            }
            if (pathPart.Index != -1) {
                builder.AppendFormat("%v", pathPart.Index);
            }
            endedWithDelimiter = false;
        }
    }

    TString contextRowKeyPath = builder.Flush();
    if (!contextRowKeyPath.empty()) {
        attributes.setItem("row_key_path", Py::ConvertToPythonString(contextRowKeyPath));
    }

    Py::Dict options;
    options.setItem("message", Py::ConvertToPythonString(TString(message)));
    options.setItem("code", Py::Long(1));
    options.setItem("attributes", attributes);

    auto ysonError = ysonErrorClass.apply(Py::Tuple(), options);
    return Py::Exception(*ysonError.type(), ysonError);
}
///////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes)
{
    auto result = GetYsonType(className).apply(Py::TupleN(object));
    result.setAttr("attributes", attributes);
    return result;
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NPython

namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

Py::Bytes EncodeStringObject(const Py::Object& obj, const TNullable<TString>& encoding, TContext* context)
{
    if (PyUnicode_Check(obj.ptr())) {
        if (!encoding) {
            throw CreateYsonError(
                Format(
                    "Cannot encode unicode object %s to bytes "
                    "since 'encoding' parameter is None",
                    Py::Repr(obj)
                ),
                context);
        }
        return Py::Bytes(PyUnicode_AsEncodedString(obj.ptr(), ~encoding.Get(), "strict"), true);
    } else {
#if PY_MAJOR_VERSION >= 3
        if (encoding) {
            throw CreateYsonError(
                Format(
                    "Bytes object %s cannot be encoded to %s. "
                    "Only unicode strings are expected if 'encoding' "
                    "parameter is not None",
                    Py::Repr(obj),
                    encoding
                ),
                context);
        }
#endif
        return Py::Bytes(PyObject_Bytes(*obj), true);
    }
}

void SerializeMapFragment(
    const Py::Object& map,
    IYsonConsumer* consumer,
    const TNullable<TString> &encoding,
    bool ignoreInnerAttributes,
    EYsonType ysonType,
    int depth,
    TContext* context)
{
    auto items = Py::Object(PyDict_CheckExact(*map) ? PyDict_Items(*map) : PyMapping_Items(*map), true);
    auto iterator = Py::Object(PyObject_GetIter(*items), true);
    while (auto* item = PyIter_Next(*iterator)) {
        auto itemGuard = Finally([item] () { Py::_XDECREF(item); });

        auto key = Py::Object(PyTuple_GET_ITEM(item, 0), false);
        auto value = Py::Object(PyTuple_GET_ITEM(item, 1), false);

        if (!PyBytes_Check(key.ptr()) && !PyUnicode_Check(key.ptr())) {
            throw CreateYsonError(Format("Map key should be string, found '%s'", Py::Repr(key)), context);
        }

        auto encodedKey = EncodeStringObject(key, encoding, context);
        auto mapKey = ConvertToStringBuf(encodedKey);
        consumer->OnKeyedItem(mapKey);
        context->Push(mapKey);
        Serialize(value, consumer, encoding, ignoreInnerAttributes, ysonType, depth + 1, context);
        context->Pop();
    }
}

void SerializePythonInteger(const Py::Object& obj, IYsonConsumer* consumer, TContext* context)
{
    static Py::Callable YsonBooleanClass;
    if (YsonBooleanClass.isNone()) {
        YsonBooleanClass = GetYsonType("YsonBoolean");
    }
    static Py::Callable YsonUint64Class;
    if (YsonUint64Class.isNone()) {
        YsonUint64Class = GetYsonType("YsonUint64");
    }
    static Py::Callable YsonInt64Class;
    if (YsonInt64Class.isNone()) {
        YsonInt64Class = GetYsonType("YsonInt64");
    }
    static Py::LongLong SignedInt64Min(std::numeric_limits<i64>::min());
    static Py::LongLong SignedInt64Max(std::numeric_limits<i64>::max());
    static Py::LongLong UnsignedInt64Max(std::numeric_limits<ui64>::max());

    if (PyObject_RichCompareBool(UnsignedInt64Max.ptr(), obj.ptr(), Py_LT) == 1 ||
        PyObject_RichCompareBool(obj.ptr(), SignedInt64Min.ptr(), Py_LT) == 1)
    {
        throw CreateYsonError(
            Format(
                "Integer %s cannot be serialized to YSON since it is out of range [-2^63, 2^64 - 1]",
                Py::Repr(obj)
            ),
            context);
    }

    auto consumeAsLong = [&] {
        int greaterThanInt64 = PyObject_RichCompareBool(SignedInt64Max.ptr(), obj.ptr(), Py_LT);

        if (greaterThanInt64 == 1) {
            auto value = PyLong_AsUnsignedLongLong(obj.ptr());
            if (PyErr_Occurred()) {
                throw Py::Exception();
            }
            consumer->OnUint64Scalar(value);
        } else if (greaterThanInt64 == 0) {
            auto value = PyLong_AsLongLong(obj.ptr());
            if (PyErr_Occurred()) {
                throw Py::Exception();
            }
            consumer->OnInt64Scalar(value);
        } else {
            Y_UNREACHABLE();
        }
    };

    if (PyLong_CheckExact(obj.ptr())) {
        consumeAsLong();
    } else if (IsInstance(obj, YsonBooleanClass)) {
        // YsonBoolean inherited from int
        consumer->OnBooleanScalar(Py::Boolean(obj));
    } else if (IsInstance(obj, YsonUint64Class)) {
        auto value = static_cast<ui64>(Py::LongLong(obj));
        if (PyErr_Occurred()) {
            throw CreateYsonError("Can not dump negative integer as YSON uint64", context);
        }
        consumer->OnUint64Scalar(value);
    } else if (IsInstance(obj, YsonInt64Class)) {
        auto value = static_cast<i64>(Py::LongLong(obj));
        if (PyErr_Occurred()) {
            throw CreateYsonError("Can not dump integer as YSON int64", context);
        }
        consumer->OnInt64Scalar(value);
    } else {
        // Object 'obj' is of some type inhereted from integer.
        consumeAsLong();
    }
}

void Serialize(
    const Py::Object& obj,
    IYsonConsumer* consumer,
    const TNullable<TString>& encoding,
    bool ignoreInnerAttributes,
    EYsonType ysonType,
    int depth,
    TContext* context)
{
    static Py::Callable YsonEntityClass;
    if (YsonEntityClass.isNone()) {
        YsonEntityClass = GetYsonType("YsonEntity");
    }

    const char* attributesStr = "attributes";
    if ((!ignoreInnerAttributes || depth == 0) && obj.hasAttr(attributesStr)) {
        auto attributeObject = obj.getAttr(attributesStr);
        if ((!attributeObject.isMapping() && !attributeObject.isNone()) || attributeObject.isSequence())  {
            throw CreateYsonError("Invalid field 'attributes', it is neither mapping nor None", context);
        }
        if (!attributeObject.isNone()) {
            auto attributes = Py::Mapping(attributeObject);
            if (attributes.length() > 0) {
                consumer->OnBeginAttributes();
                context->PushAttributesStarted();
                SerializeMapFragment(attributes, consumer, encoding, ignoreInnerAttributes, ysonType, depth, context);
                context->Pop();
                consumer->OnEndAttributes();
            }
        }
    }

    if (PyBytes_Check(obj.ptr()) || PyUnicode_Check(obj.ptr())) {
        consumer->OnStringScalar(ConvertToStringBuf(EncodeStringObject(obj, encoding, context)));
#if PY_MAJOR_VERSION < 3
    // Fast check for simple integers (python 3 has only long integers)
    } else if (PyInt_CheckExact(obj.ptr())) {
        consumer->OnInt64Scalar(Py::ConvertToLongLong(obj));
#endif
    } else if (obj.isBoolean()) {
        consumer->OnBooleanScalar(Py::Boolean(obj));
    } else if (Py::IsInteger(obj)) {
        SerializePythonInteger(obj, consumer, context);
    } else if (obj.isMapping() && obj.hasAttr("items")) {
        bool allowBeginEnd =  depth > 0 || ysonType != NYson::EYsonType::MapFragment;
        if (allowBeginEnd) {
            consumer->OnBeginMap();
        }
        SerializeMapFragment(obj, consumer, encoding, ignoreInnerAttributes, ysonType, depth, context);
        if (allowBeginEnd) {
            consumer->OnEndMap();
        }
    } else if (obj.isSequence()) {
        const auto& objList = Py::Sequence(obj);
        consumer->OnBeginList();
        int index = 0;
        for (auto it = objList.begin(); it != objList.end(); ++it) {
            consumer->OnListItem();
            context->Push(index);
            Serialize(*it, consumer, encoding, ignoreInnerAttributes, ysonType, depth + 1, context);
            context->Pop();
            ++index;
        }
        consumer->OnEndList();
    } else if (Py::IsFloat(obj)) {
        consumer->OnDoubleScalar(Py::Float(obj));
    } else if (obj.isNone() || IsInstance(obj, YsonEntityClass)) {
        consumer->OnEntity();
    } else {
        throw CreateYsonError(
            Format(
                "Value %s cannot be serialized to YSON since it has unsupported type",
                Py::Repr(obj)
            ),
            context);
    }
}

////////////////////////////////////////////////////////////////////////////////

TPythonObjectBuilder::TPythonObjectBuilder(bool alwaysCreateAttributes, const TNullable<TString>& encoding)
    : YsonMap(GetYsonType("YsonMap"))
    , YsonList(GetYsonType("YsonList"))
    , YsonString(GetYsonType("YsonString"))
#if PY_MAJOR_VERSION >= 3
    , YsonUnicode(GetYsonType("YsonUnicode"))
#endif
    , YsonInt64(GetYsonType("YsonInt64"))
    , YsonUint64(GetYsonType("YsonUint64"))
    , YsonDouble(GetYsonType("YsonDouble"))
    , YsonBoolean(GetYsonType("YsonBoolean"))
    , YsonEntity(GetYsonType("YsonEntity"))
    , AlwaysCreateAttributes_(alwaysCreateAttributes)
    , Encoding_(encoding)
{ }

TPythonObjectBuilder::PyObjectPtr TPythonObjectBuilder::MakePyObjectPtr(PyObject* obj)
{
    return std::unique_ptr<PyObject, decltype(&Py::_XDECREF)>(obj, &Py::_XDECREF);
}

void TPythonObjectBuilder::OnStringScalar(const TStringBuf& value)
{
    auto bytes = MakePyObjectPtr(PyBytes_FromStringAndSize(~value, value.size()));
    if (!bytes) {
        throw Py::Exception();
    }

    if (Encoding_) {
        auto decodedString = MakePyObjectPtr(
            PyUnicode_FromEncodedObject(bytes.get(), ~Encoding_.Get(), "strict"));
        if (!decodedString) {
            throw Py::Exception();
        }
#if PY_MAJOR_VERSION < 3
        auto utf8String = MakePyObjectPtr(PyUnicode_AsUTF8String(decodedString.get()));
        AddObject(std::move(utf8String), YsonString);
#else
        AddObject(std::move(decodedString), YsonUnicode);
#endif
    } else {
        AddObject(std::move(bytes), YsonString);
    }
}

void TPythonObjectBuilder::OnInt64Scalar(i64 value)
{
    AddObject(MakePyObjectPtr(PyLong_FromLongLong(value)), YsonInt64);
}

void TPythonObjectBuilder::OnUint64Scalar(ui64 value)
{
    AddObject(MakePyObjectPtr(PyLong_FromUnsignedLongLong(value)), YsonUint64, EPythonObjectType::Other, true);
}

void TPythonObjectBuilder::OnDoubleScalar(double value)
{
    AddObject(MakePyObjectPtr(PyFloat_FromDouble(value)), YsonDouble);
}

void TPythonObjectBuilder::OnBooleanScalar(bool value)
{
    AddObject(MakePyObjectPtr(PyBool_FromLong(value ? 1 : 0)), YsonBoolean);
}

void TPythonObjectBuilder::OnEntity()
{
    AddObject(MakePyObjectPtr(Py::new_reference_to(Py_None)), YsonEntity);
}

void TPythonObjectBuilder::OnBeginList()
{
    AddObject(MakePyObjectPtr(PyList_New(0)), YsonList, EPythonObjectType::List);
}

void TPythonObjectBuilder::OnListItem()
{
}

void TPythonObjectBuilder::OnEndList()
{
    Pop();
}

void TPythonObjectBuilder::OnBeginMap()
{
    AddObject(MakePyObjectPtr(PyDict_New()), YsonMap, EPythonObjectType::Map);
}

void TPythonObjectBuilder::OnKeyedItem(const TStringBuf& key)
{
    PyObject* pyKey = nullptr;

    auto it = KeyCache_.find(key);
    if (it == KeyCache_.end()) {
        auto pyKeyPtr = MakePyObjectPtr(PyBytes_FromStringAndSize(~key, key.size()));
        if (!pyKeyPtr) {
            throw Py::Exception();
        }

        auto ownedKey = ConvertToStringBuf(Py::Bytes(pyKeyPtr.get()));

        if (Encoding_) {
            auto originalPyKeyObj = pyKeyPtr.get();
            OriginalKeyCache_.emplace_back(std::move(pyKeyPtr));

            pyKeyPtr = MakePyObjectPtr(
                PyUnicode_FromEncodedObject(originalPyKeyObj, ~Encoding_.Get(), "strict"));
            if (!pyKeyPtr) {
                throw Py::Exception();
            }
        }

        auto res = KeyCache_.emplace(ownedKey, std::move(pyKeyPtr));
        YCHECK(res.second);
        pyKey = res.first->second.get();
    } else {
        pyKey = it->second.get();
    }

    Keys_.push(pyKey);
}

void TPythonObjectBuilder::OnEndMap()
{
    Pop();
}

void TPythonObjectBuilder::OnBeginAttributes()
{
    Push(MakePyObjectPtr(PyDict_New()), EPythonObjectType::Attributes);
}

void TPythonObjectBuilder::OnEndAttributes()
{
    Attributes_ = Pop();
}

void TPythonObjectBuilder::AddObject(
    PyObjectPtr obj,
    const Py::Callable& type,
    EPythonObjectType objType,
    bool forceYsonTypeCreation)
{
    static const char* attributesStr = "attributes";

    if (!obj) {
        throw Py::Exception();
    }

    if (Attributes_ || forceYsonTypeCreation || AlwaysCreateAttributes_) {
        auto tuplePtr = MakePyObjectPtr(PyTuple_New(1));
        PyTuple_SetItem(tuplePtr.get(), 0, Py::new_reference_to(obj.get()));
        obj = MakePyObjectPtr(PyObject_CallObject(type.ptr(), tuplePtr.get()));
        if (!obj.get()) {
            throw Py::Exception();
        }
    }

    if (Attributes_) {
        PyObject_SetAttrString(obj.get(), attributesStr, Attributes_->get());
        Attributes_ = Null;
    }

    if (ObjectStack_.empty()) {
        Objects_.push(Py::Object(obj.get()));
    } else if (ObjectStack_.top().second == EPythonObjectType::List) {
        PyList_Append(ObjectStack_.top().first.get(), obj.get());
    } else {
        PyDict_SetItem(ObjectStack_.top().first.get(), Keys_.top(), obj.get());
        Keys_.pop();
    }

    if (objType == EPythonObjectType::List || objType == EPythonObjectType::Map) {
        Push(std::move(obj), objType);
    }
}

void TPythonObjectBuilder::Push(PyObjectPtr objPtr, EPythonObjectType objectType)
{
    ObjectStack_.emplace(std::move(objPtr), objectType);
}

TPythonObjectBuilder::PyObjectPtr TPythonObjectBuilder::Pop()
{
    auto obj = std::move(ObjectStack_.top().first);
    ObjectStack_.pop();
    return obj;
}


Py::Object TPythonObjectBuilder::ExtractObject()
{
    auto obj = Objects_.front();
    Objects_.pop();
    return obj;
}

bool TPythonObjectBuilder::HasObject() const
{
    return Objects_.size() > 1 || (Objects_.size() == 1 && ObjectStack_.size() == 0);
}

////////////////////////////////////////////////////////////////////////////////


TGilGuardedYsonConsumer::TGilGuardedYsonConsumer(IYsonConsumer* consumer)
    : Consumer_(consumer)
{ }

void TGilGuardedYsonConsumer::OnStringScalar(const TStringBuf& value)
{
    NPython::TGilGuard guard;
    Consumer_->OnStringScalar(value);
}

void TGilGuardedYsonConsumer::OnInt64Scalar(i64 value)
{
    NPython::TGilGuard guard;
    Consumer_->OnInt64Scalar(value);
}

void TGilGuardedYsonConsumer::OnUint64Scalar(ui64 value)
{
    NPython::TGilGuard guard;
    Consumer_->OnUint64Scalar(value);
}

void TGilGuardedYsonConsumer::OnDoubleScalar(double value)
{
    NPython::TGilGuard guard;
    Consumer_->OnDoubleScalar(value);
}

void TGilGuardedYsonConsumer::OnBooleanScalar(bool value)
{
    NPython::TGilGuard guard;
    Consumer_->OnBooleanScalar(value);
}

void TGilGuardedYsonConsumer::OnEntity()
{
    NPython::TGilGuard guard;
    Consumer_->OnEntity();
}

void TGilGuardedYsonConsumer::OnBeginList()
{
    NPython::TGilGuard guard;
    Consumer_->OnBeginList();
}

void TGilGuardedYsonConsumer::OnListItem()
{
    NPython::TGilGuard guard;
    Consumer_->OnListItem();
}

void TGilGuardedYsonConsumer::OnEndList()
{
    NPython::TGilGuard guard;
    Consumer_->OnEndList();
}

void TGilGuardedYsonConsumer::OnBeginMap()
{
    NPython::TGilGuard guard;
    Consumer_->OnBeginMap();
}

void TGilGuardedYsonConsumer::OnKeyedItem(const TStringBuf& key)
{
    NPython::TGilGuard guard;
    Consumer_->OnKeyedItem(key);
}

void TGilGuardedYsonConsumer::OnEndMap()
{
    NPython::TGilGuard guard;
    Consumer_->OnEndMap();
}

void TGilGuardedYsonConsumer::OnBeginAttributes()
{
    NPython::TGilGuard guard;
    Consumer_->OnBeginAttributes();
}

void TGilGuardedYsonConsumer::OnEndAttributes()
{
    NPython::TGilGuard guard;
    Consumer_->OnEndAttributes();
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(Py::Object& obj, INodePtr node, const TNullable<TString>& encoding)
{
    Py::Object attributes = Py::Dict();
    if (!node->Attributes().List().empty()) {
        Deserialize(attributes, node->Attributes().ToMap(), encoding);
    }

    auto type = node->GetType();
    if (type == ENodeType::Map) {
        auto map = Py::Dict();
        for (auto child : node->AsMap()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child.second, encoding);
            map.setItem(~child.first, item);
        }
        obj = NPython::CreateYsonObject("YsonMap", map, attributes);
    } else if (type == ENodeType::Entity) {
        obj = NPython::CreateYsonObject("YsonEntity", Py::None(), attributes);
    } else if (type == ENodeType::Boolean) {
        obj = NPython::CreateYsonObject("YsonBoolean", Py::Boolean(node->AsBoolean()->GetValue()), attributes);
    } else if (type == ENodeType::Int64) {
        obj = NPython::CreateYsonObject("YsonInt64", Py::Int(node->AsInt64()->GetValue()), attributes);
    } else if (type == ENodeType::Uint64) {
        obj = NPython::CreateYsonObject("YsonUint64", Py::LongLong(node->AsUint64()->GetValue()), attributes);
    } else if (type == ENodeType::Double) {
        obj = NPython::CreateYsonObject("YsonDouble", Py::Float(node->AsDouble()->GetValue()), attributes);
    } else if (type == ENodeType::String) {
        auto str = Py::Bytes(~node->AsString()->GetValue());
        if (encoding) {
#if PY_MAJOR_VERSION >= 3
            obj = NPython::CreateYsonObject("YsonUnicode", str.decode(~encoding.Get()), attributes);
#else
            obj = NPython::CreateYsonObject("YsonString", str.decode(~encoding.Get()).encode("utf-8"), attributes);
#endif
        } else {
            obj = NPython::CreateYsonObject("YsonString", Py::Bytes(~node->AsString()->GetValue()), attributes);
        }
    } else if (type == ENodeType::List) {
        auto list = Py::List();
        for (auto child : node->AsList()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child, encoding);
            list.append(item);
        }
        obj = NPython::CreateYsonObject("YsonList", list, attributes);
    } else {
        THROW_ERROR_EXCEPTION("Unsupported node type %s", ~ToString(type));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TInputStreamBlobTag { };

class TStreamReader
{
public:
    explicit TStreamReader(TInputStream* stream)
        : Stream_(stream)
    {
        ReadNextBlob();
        if (!Finished_) {
            RefreshBlock();
        }
    }

    const char* Begin() const
    {
        return BeginPtr_;
    }

    const char* End() const
    {
        return EndPtr_;
    }

    void RefreshBlock()
    {
        YCHECK(BeginPtr_ == EndPtr_);
        YCHECK(!Finished_);

        Blobs_.push_back(NextBlob_);
        BeginPtr_ = NextBlob_.Begin();
        EndPtr_ = NextBlob_.Begin() + NextBlobSize_;

        if (NextBlobSize_ < BlockSize_) {
            Finished_ = true;
        } else {
            ReadNextBlob();
        }
    }

    void Advance(size_t bytes)
    {
        BeginPtr_ += bytes;
        ReadByteCount_ += bytes;
    }

    bool IsFinished() const
    {
        return Finished_;
    }

    TSharedRef ExtractPrefix()
    {
        YCHECK(!Blobs_.empty());

        if (!PrefixStart_) {
            PrefixStart_ = Blobs_.front().Begin();
        }

        TSharedMutableRef result;

        if (Blobs_.size() == 1) {
            result = Blobs_[0].Slice(PrefixStart_, BeginPtr_);
        } else {
            result = TSharedMutableRef::Allocate<TInputStreamBlobTag>(ReadByteCount_, false);

            size_t index = 0;
            auto append = [&] (const char* begin, const char* end) {
                std::copy(begin, end, result.Begin() + index);
                index += end - begin;
            };

            append(PrefixStart_, Blobs_.front().End());
            for (int i = 1; i + 1 < Blobs_.size(); ++i) {
                append(Blobs_[i].Begin(), Blobs_[i].End());
            }
            append(Blobs_.back().Begin(), BeginPtr_);

            while (Blobs_.size() > 1) {
                Blobs_.pop_front();
            }
        }

        PrefixStart_ = BeginPtr_;
        ReadByteCount_ = 0;

        return result;
    }

private:
    TInputStream* Stream_;

    std::deque<TSharedMutableRef> Blobs_;

    TSharedMutableRef NextBlob_;
    i64 NextBlobSize_ = 0;

    char* BeginPtr_ = nullptr;
    char* EndPtr_ = nullptr;

    char* PrefixStart_ = nullptr;
    i64 ReadByteCount_ = 0;

    bool Finished_ = false;
    static const size_t BlockSize_ = 1024 * 1024;

    void ReadNextBlob()
    {
        NextBlob_ = TSharedMutableRef::Allocate<TInputStreamBlobTag>(BlockSize_, false);
        NextBlobSize_ = Stream_->Load(NextBlob_.Begin(), NextBlob_.Size());
        if (NextBlobSize_ == 0) {
            Finished_ = true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TListFragmentLexer::TImpl
{
public:
    explicit TImpl(TInputStream* stream)
        : Lexer_(TStreamReader(stream))
    { }

    TSharedRef NextItem()
    {
        int balance = 0;
        bool finished = false;
        TToken token;

        bool hasRow = false;
        while (!finished) {
            Lexer_.GetToken(&token);
            auto type = token.GetType();

            switch (type) {
                case ETokenType::EndOfStream:
                    finished = true;
                    break;
                case ETokenType::LeftBracket:
                case ETokenType::LeftBrace:
                case ETokenType::LeftAngle:
                    balance += 1;
                    break;
                case ETokenType::RightBracket:
                case ETokenType::RightBrace:
                case ETokenType::RightAngle:
                    balance -= 1;
                    if (balance == 0) {
                        hasRow = true;
                    }
                    break;
                case ETokenType::Semicolon:
                    if (balance == 0) {
                        return Lexer_.ExtractPrefix();
                    }
                    break;
                case ETokenType::String:
                case ETokenType::Int64:
                case ETokenType::Uint64:
                case ETokenType::Double:
                case ETokenType::Boolean:
                case ETokenType::Hash:
                case ETokenType::Equals:
                    if (balance == 0) {
                        hasRow = true;
                    }
                    break;
                default:
                    THROW_ERROR_EXCEPTION("Unexpected token %Qv in YSON list fragment", token);
            }
        }

        if (balance != 0) {
            THROW_ERROR_EXCEPTION("YSON list fragment is incomplete");
        }
        if (hasRow) {
            auto prefix = Lexer_.ExtractPrefix();
            YCHECK(*(prefix.End() - 1) != NYson::NDetail::ItemSeparatorSymbol);
            auto result = TSharedMutableRef::Allocate(prefix.Size() + 1, false);
            std::copy(prefix.Begin(), prefix.End(), result.Begin());
            *(result.End() - 1) = ';';
            return result;
        }

        return TSharedRef();
    }

private:
    NYson::NDetail::TLexer<TStreamReader, true> Lexer_;
};

////////////////////////////////////////////////////////////////////////////////

TListFragmentLexer::TListFragmentLexer()
{ }

TListFragmentLexer::~TListFragmentLexer()
{ }

TListFragmentLexer::TListFragmentLexer(TListFragmentLexer&& lexer)
    : Impl_(std::move(lexer.Impl_))
{ }

TListFragmentLexer& TListFragmentLexer::operator=(TListFragmentLexer&& lexer)
{
    Impl_ = std::move(lexer.Impl_);
    return *this;
}

TListFragmentLexer::TListFragmentLexer(TInputStream* stream)
    : Impl_(new TImpl(stream))
{ }

TSharedRef TListFragmentLexer::NextItem()
{
    return Impl_->NextItem();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
