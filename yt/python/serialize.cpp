#include "helpers.h"
#include "serialize.h"

#include <core/ytree/node.h>

#include <core/yson/lexer_detail.h>

#include <numeric>

namespace NYT {

namespace {

///////////////////////////////////////////////////////////////////////////////

using NYson::TToken;
using NYson::ETokenType;
using NYson::IYsonConsumer;
using NYTree::INodePtr;
using NYTree::ENodeType;

///////////////////////////////////////////////////////////////////////////////

Py::Callable GetYsonType(const std::string& name)
{
    // TODO(ignat): make singleton
    static PyObject* ysonTypesModule = nullptr;
    if (!ysonTypesModule) {
        ysonTypesModule = PyImport_ImportModule("yt.yson.yson_types");
        if (!ysonTypesModule) {
            throw Py::RuntimeError("Failed to import module yt.yson.yson_types");
        }
    }
    return Py::Callable(PyObject_GetAttrString(ysonTypesModule, name.c_str()));
}

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes)
{
    auto result = GetYsonType(className).apply(Py::TupleN(object));
    result.setAttr("attributes", attributes);
    return result;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NYTree {

///////////////////////////////////////////////////////////////////////////////

void SerializeMapFragment(const Py::Object& map, IYsonConsumer* consumer, bool ignoreInnerAttributes, int depth)
{
    auto items = Py::Object(PyDict_CheckExact(*map) ? PyDict_Items(*map) : PyMapping_Items(*map), true);
    auto iterator = Py::Object(PyObject_GetIter(*items), true);
    while (auto* item = PyIter_Next(*iterator)) {
         auto key = Py::Object(PyTuple_GET_ITEM(item, 0), false);
         char* keyStr = PyString_AsString(ConvertToString(key).ptr());
         auto value = Py::Object(PyTuple_GET_ITEM(item, 1), false);
         consumer->OnKeyedItem(TStringBuf(keyStr));
         Serialize(value, consumer, ignoreInnerAttributes, depth + 1);
         Py::_XDECREF(item);
    }
}

void SerializePythonInteger(const Py::Object& obj, IYsonConsumer* consumer)
{
    static Py::Callable YsonBooleanClass = GetYsonType("YsonBoolean");
    static Py::Callable YsonUint64Class = GetYsonType("YsonUint64");
    static Py::Callable YsonInt64Class = GetYsonType("YsonInt64");
    static Py::LongLong SignedInt64Min(std::numeric_limits<i64>::min());
    static Py::LongLong SignedInt64Max(std::numeric_limits<i64>::max());
    static Py::LongLong UnsignedInt64Max(std::numeric_limits<ui64>::max());

    if (PyObject_Compare(UnsignedInt64Max.ptr(), obj.ptr()) < 0 ||
        PyObject_Compare(obj.ptr(), SignedInt64Min.ptr()) < 0)
    {
        throw Py::RuntimeError(
            "Integer " + std::string(obj.repr()) +
            " cannot be serialized to YSON since it is out of range [-2^63, 2^64 - 1]");
    }

    auto consumeAsLong = [&] {
        bool greaterThanInt64 = PyObject_Compare(SignedInt64Max.ptr(), obj.ptr()) < 0;
        if (greaterThanInt64) {
            consumer->OnUint64Scalar(PyLong_AsUnsignedLongLong(obj.ptr()));
        } else {
            consumer->OnInt64Scalar(PyLong_AsLongLong(obj.ptr()));
        }
    };

    if (PyLong_CheckExact(obj.ptr())) {
        consumeAsLong();
        return;
    }

    // YsonBoolean inherited from int
    if (IsInstance(obj, YsonBooleanClass)) {
        consumer->OnBooleanScalar(Py::Boolean(obj));
        return;
    }

    bool isUint64 = IsInstance(obj, YsonUint64Class);
    if (isUint64) {
        bool negative = PyObject_Compare(Py::Int(0).ptr(), obj.ptr()) > 0;
        if (negative) {
            throw Py::RuntimeError("Can not dump negative integer as YSON uint64");
        }
        consumer->OnUint64Scalar(PyLong_AsUnsignedLongLong(obj.ptr()));
        return;
    }

    bool isInt64 = IsInstance(obj, YsonInt64Class);
    if (isInt64) {
        bool greaterThanInt64 = PyObject_Compare(SignedInt64Max.ptr(), obj.ptr()) < 0;
        if (greaterThanInt64) {
            throw Py::RuntimeError("Can not dump integer greater than 2^63-1 as YSON int64");
        }
        consumer->OnInt64Scalar(PyLong_AsLongLong(obj.ptr()));
        return;
    }

    consumeAsLong();
}

void Serialize(const Py::Object& obj, IYsonConsumer* consumer, bool ignoreInnerAttributes, int depth)
{
    static Py::Callable YsonEntityClass = GetYsonType("YsonEntity");

    const char* attributesStr = "attributes";
    if ((!ignoreInnerAttributes || depth == 0) && PyObject_HasAttrString(*obj, attributesStr)) {
        auto attributes = Py::Mapping(PyObject_GetAttrString(*obj, attributesStr), true);
        if (attributes.length() > 0) {
            consumer->OnBeginAttributes();
            SerializeMapFragment(attributes, consumer, ignoreInnerAttributes, depth);
            consumer->OnEndAttributes();
        }
    }

    if (PyString_Check(obj.ptr())) {
        consumer->OnStringScalar(ConvertToStringBuf(ConvertToString(obj)));
    } else if (PyUnicode_Check(obj.ptr())) {
        Py::String encoded = Py::String(PyUnicode_AsUTF8String(obj.ptr()), true);
        consumer->OnStringScalar(ConvertToStringBuf(ConvertToString(encoded)));
    } else if (obj.isMapping()) {
        consumer->OnBeginMap();
        SerializeMapFragment(obj, consumer, ignoreInnerAttributes, depth);
        consumer->OnEndMap();
    // Fast check for simple integers
    } else if (PyInt_CheckExact(obj.ptr())) {
        consumer->OnInt64Scalar(PyLong_AsLongLong(obj.ptr()));
    } else if (obj.isBoolean()) {
        consumer->OnBooleanScalar(Py::Boolean(obj));
    } else if (obj.isInteger() or PyLong_Check(obj.ptr())) {
        SerializePythonInteger(obj, consumer);
    } else if (obj.isSequence()) {
        const auto& objList = Py::Sequence(obj);
        consumer->OnBeginList();
        for (auto it = objList.begin(); it != objList.end(); ++it) {
            consumer->OnListItem();
            Serialize(*it, consumer, ignoreInnerAttributes, depth + 1);
        }
        consumer->OnEndList();
    } else if (obj.isFloat()) {
        consumer->OnDoubleScalar(Py::Float(obj));
    } else if (obj.isNone() || IsInstance(obj, YsonEntityClass)) {
        consumer->OnEntity();
    } else {
        throw Py::RuntimeError(
            "Value " + std::string(obj.repr()) +
            " cannot be serialized to YSON since it has unsupported type");
    }
}

///////////////////////////////////////////////////////////////////////////////

TPythonObjectBuilder::TPythonObjectBuilder(bool alwaysCreateAttributes)
    : YsonMap(GetYsonType("YsonMap"))
    , YsonList(GetYsonType("YsonList"))
    , YsonString(GetYsonType("YsonString"))
    , YsonInt64(GetYsonType("YsonInt64"))
    , YsonUint64(GetYsonType("YsonUint64"))
    , YsonDouble(GetYsonType("YsonDouble"))
    , YsonBoolean(GetYsonType("YsonBoolean"))
    , YsonEntity(GetYsonType("YsonEntity"))
    , AlwaysCreateAttributes_(alwaysCreateAttributes)
{ }

void TPythonObjectBuilder::OnStringScalar(const TStringBuf& value)
{
    Py::_XDECREF(AddObject(PyString_FromStringAndSize(~value, value.size()), YsonString));
}

void TPythonObjectBuilder::OnInt64Scalar(i64 value)
{
    Py::_XDECREF(AddObject(PyLong_FromLongLong(value), YsonInt64));
}

void TPythonObjectBuilder::OnUint64Scalar(ui64 value)
{
    Py::_XDECREF(AddObject(PyLong_FromUnsignedLongLong(value), YsonUint64, true));
}

void TPythonObjectBuilder::OnDoubleScalar(double value)
{
    Py::_XDECREF(AddObject(PyFloat_FromDouble(value), YsonDouble));
}

void TPythonObjectBuilder::OnBooleanScalar(bool value)
{
    Py::_XDECREF(AddObject(PyBool_FromLong(value ? 1 : 0), YsonBoolean));
}

void TPythonObjectBuilder::OnEntity()
{
    Py_INCREF(Py_None);
    Py::_XDECREF(AddObject(Py_None, YsonEntity));
}

void TPythonObjectBuilder::OnBeginList()
{
    auto obj = AddObject(PyList_New(0), YsonList);
    Push(Py::Object(obj, true), EPythonObjectType::List);
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
    auto obj = AddObject(PyDict_New(), YsonMap);
    Push(Py::Object(obj, true), EPythonObjectType::Map);
}

void TPythonObjectBuilder::OnKeyedItem(const TStringBuf& key)
{
    Keys_.push(Stroka(key));
}

void TPythonObjectBuilder::OnEndMap()
{
    Pop();
}

void TPythonObjectBuilder::OnBeginAttributes()
{
    auto obj = Py::Dict();
    Push(obj, EPythonObjectType::Attributes);
}

void TPythonObjectBuilder::OnEndAttributes()
{
    Attributes_ = Pop();
}

PyObject* TPythonObjectBuilder::AddObject(PyObject* obj, const Py::Callable& type, bool forceYsonTypeCreation)
{
    if ((AlwaysCreateAttributes_ && !Attributes_) || forceYsonTypeCreation) {
        Attributes_ = Py::Dict();
    }

    if (Attributes_) {
        auto ysonObj = type.apply(Py::TupleN(Py::Object(obj)));
        auto ysonObjPtr = ysonObj.ptr();
        Py::_XDECREF(obj);
        Py::_XINCREF(ysonObjPtr);
        return AddObject(ysonObjPtr);
    } else {
        return AddObject(obj);
    }
}

PyObject* TPythonObjectBuilder::AddObject(const Py::Callable& type)
{
    auto ysonObj = type.apply(Py::Tuple());
    auto ysonObjPtr = ysonObj.ptr();
    Py::_XINCREF(ysonObjPtr);
    return AddObject(ysonObjPtr);
}

PyObject* TPythonObjectBuilder::AddObject(PyObject* obj)
{
    static const char* attributes = "attributes";
    if (Attributes_) {
        PyObject_SetAttrString(obj, const_cast<char*>(attributes), (*Attributes_).ptr());
        Attributes_ = Null;
    }

    if (ObjectStack_.empty()) {
        Objects_.push(Py::Object(obj));
    } else if (ObjectStack_.top().second == EPythonObjectType::List) {
        PyList_Append(ObjectStack_.top().first.ptr(), obj);
    } else {
        auto keyObj = PyString_FromStringAndSize(~Keys_.top(), Keys_.top().size());
        PyDict_SetItem(*ObjectStack_.top().first, keyObj, obj);
        Keys_.pop();
        Py::_XDECREF(keyObj);
    }

    return obj;
}

void TPythonObjectBuilder::Push(const Py::Object& obj, EPythonObjectType objectType)
{
    ObjectStack_.emplace(obj, objectType);
}

Py::Object TPythonObjectBuilder::Pop()
{
    auto obj = ObjectStack_.top().first;
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

///////////////////////////////////////////////////////////////////////////////


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

///////////////////////////////////////////////////////////////////////////////

void Deserialize(Py::Object& obj, INodePtr node)
{
    Py::Object attributes = Py::Dict();
    if (!node->Attributes().List().empty()) {
        Deserialize(attributes, node->Attributes().ToMap());
    }

    auto type = node->GetType();
    if (type == ENodeType::Map) {
        auto map = Py::Dict();
        for (auto child : node->AsMap()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child.second);
            map.setItem(~child.first, item);
        }
        obj = CreateYsonObject("YsonMap", map, attributes);
    } else if (type == ENodeType::Entity) {
        obj = CreateYsonObject("YsonEntity", Py::None(), attributes);
    } else if (type == ENodeType::Boolean) {
        obj = CreateYsonObject("YsonBoolean", Py::Boolean(node->AsBoolean()->GetValue()), attributes);
    } else if (type == ENodeType::Int64) {
        obj = CreateYsonObject("YsonInt64", Py::Int(node->AsInt64()->GetValue()), attributes);
    } else if (type == ENodeType::Uint64) {
        obj = CreateYsonObject("YsonUint64", Py::LongLong(node->AsUint64()->GetValue()), attributes);
    } else if (type == ENodeType::Double) {
        obj = CreateYsonObject("YsonDouble", Py::Float(node->AsDouble()->GetValue()), attributes);
    } else if (type == ENodeType::String) {
        obj = CreateYsonObject("YsonString", Py::String(~node->AsString()->GetValue()), attributes);
    } else if (type == ENodeType::List) {
        auto list = Py::List();
        for (auto child : node->AsList()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child);
            list.append(item);
        }
        obj = CreateYsonObject("YsonList", list, attributes);
    } else {
        THROW_ERROR_EXCEPTION("Unsupported node type %s", ~ToString(type));
    }
}

///////////////////////////////////////////////////////////////////////////////

struct TInputStreamBlobTag { };

class TStreamReader
{
public:
    explicit TStreamReader(TInputStream* stream)
        : Stream_(stream)
    {
        RefreshBlock();
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
        auto blob = TSharedMutableRef::Allocate<TInputStreamBlobTag>(BlockSize_, false);
        auto size = Stream_->Load(blob.Begin(), blob.Size());
        if (size != BlockSize_) {
            Finished_ = true;
        }

        Blobs_.push_back(blob);
        BeginPtr_ = blob.Begin();
        EndPtr_ = blob.Begin() + size;
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

    char* BeginPtr_ = nullptr;
    char* EndPtr_ = nullptr;

    char* PrefixStart_ = nullptr;
    i64 ReadByteCount_ = 0;

    bool Finished_ = false;
    static const size_t BlockSize_ = 1024 * 1024;
};

////////////////////////////////////////////////////////////////////////////////

class TListFragmentLexer::TImpl
{
public:
    explicit TImpl(TInputStream* stream)
        : Lexer_(TStreamReader(stream), Null)
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
            YCHECK(*(prefix.End() - 1) != NYson::NDetail::ListItemSeparatorSymbol);
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
