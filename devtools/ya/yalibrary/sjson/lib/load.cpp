#include "load.h"
#include "common.h"
#include "filter.h"

#include <Python.h>

#include <devtools/libs/json_sax/reader.h>

#include <library/cpp/pybind/ptr.h>

#include <util/generic/vector.h>
#include <util/stream/input.h>
#include <util/stream/str.h>


namespace NSJson {

    namespace {
        using NPyBind::TPyObjectPtr;
        constexpr bool BORROW = true;

#if PY_MAJOR_VERSION == 2
        PyObject* Py_NewRef(PyObject* obj) {
            Py_INCREF(obj);
            return obj;
        }
#endif

        inline PyObject* CheckNewObject(PyObject* val) {
            Y_ENSURE(val != nullptr);
            return val;
        }

        PyObject* MakePyObject(bool val) {
            if (val) {
                Py_RETURN_TRUE;
            }
            Py_RETURN_FALSE;
        }

        PyObject* MakePyNoneObject() {
            Py_RETURN_NONE;
        }

        PyObject* MakePyObject(unsigned long val) {
            return CheckNewObject(PyLong_FromUnsignedLong(val));
        }

        PyObject* MakePyObject(long long val) {
            return CheckNewObject(PyLong_FromLongLong(val));
        }

        PyObject* MakePyObject(unsigned long long val) {
            return CheckNewObject(PyLong_FromUnsignedLongLong(val));
        }

        PyObject* MakePyObject(double val) {
            return CheckNewObject(PyFloat_FromDouble(val));
        }

        PyObject* MakePyObject(TStringBuf str, bool intern) {
    #if PY_MAJOR_VERSION == 3
            PyObject* pyStr = CheckNewObject(PyUnicode_FromStringAndSize(str.data(), str.size()));
            if (intern) {
                PyUnicode_InternInPlace(&pyStr);
            }
            return pyStr;
    #else
            PyObject* pyStr = CheckNewObject(PyString_FromStringAndSize(str.data(), str.size()));
            if (intern) {
                PyString_InternInPlace(&pyStr);
            }
            return pyStr;
    #endif
        }

        // Note: returning value becomes invalid after obj destroying
        TStringBuf GetBinaryStringView(PyObject* obj) {
#if PY_MAJOR_VERSION == 3
            if (PyBytes_Check(obj)) {
                char* data{};
                Py_ssize_t len{};
                Y_ENSURE(PyBytes_AsStringAndSize(obj, &data, &len) != -1);
                return {data, static_cast<std::size_t>(len)};
            }
#else
            if (PyString_Check(obj)) {
                char* data{};
                Py_ssize_t len{};
                Y_ENSURE(PyString_AsStringAndSize(obj, &data, &len) != -1);
                return {data, static_cast<std::size_t>(len)};
            }
#endif
            ythrow yexception() << "Binary string is expected";
        }

        class TPythonInputStreamWrapper : public IInputStream {
        public:
            TPythonInputStreamWrapper(PyObject* stream)
                : PyStream_{stream}
                , ReadMethod_(PyObject_GetAttrString(stream, "read"), BORROW)
            {
                if (!ReadMethod_.Get()) {
                    ythrow TValueError() << "Stream doesn't have 'read' attribute";
                }
                if (!PyCallable_Check(ReadMethod_.Get())) {
                    ythrow TValueError() << "Stream 'read' attribute is not callable";
                }
            }

            std::size_t DoRead(void* buf, std::size_t len) override {
                TPyObjectPtr pyLen{MakePyObject(len), BORROW};
                TPyObjectPtr pyBytes{PyObject_CallFunctionObjArgs(ReadMethod_.Get(), pyLen.Get(), nullptr), BORROW};
                if (!pyBytes.Get()) {
                    ythrow yexception() << "read() method failed";
                }
                const TStringBuf s = GetBinaryStringView(pyBytes.Get());
                std::size_t readLen = s.length();
                Y_ASSERT(readLen <= len);
                std::memcpy(buf, s.data(), readLen);
                return readLen;
            }

        private:
            TPyObjectPtr PyStream_;
            TPyObjectPtr ReadMethod_;
        };

        struct IPyObjectHolder : TNonCopyable {
            virtual void Init(PyObject* value) = 0;
            virtual PyObject* Get() const = 0;
            virtual ~IPyObjectHolder() = default;
        };
        using TPyObjectHolderPtr = THolder<IPyObjectHolder>;

        class TPyObjectHolder : public IPyObjectHolder {
        public:
            void Init(PyObject* value) override {
                Y_ASSERT(value);
                Y_ASSERT(!Obj_);
                Obj_ = value;
            }

            PyObject* Get() const override {
                Y_ASSERT(Obj_);
                return Obj_;
            }

            PyObject* GetNewRef() const {
                Y_ASSERT(Obj_);
                return Py_NewRef(Obj_);
            }

            ~TPyObjectHolder() override {
                Py_XDECREF(Obj_);
            }

        private:
            PyObject* Obj_{};
        };

        class TPyObjectHolderProxy : public IPyObjectHolder {
        public:
            TPyObjectHolderProxy(IPyObjectHolder& objHolder)
                : ObjHolder_{objHolder}
            {
            }

            void Init(PyObject* value) override {
                ObjHolder_.Init(value);
            }

            PyObject* Get() const override {
                return ObjHolder_.Get();
            }

        private:
            IPyObjectHolder& ObjHolder_;
        };

        class TPyListItemHolder : public TPyObjectHolder {
        public:
            TPyListItemHolder(IPyObjectHolder& parentObjHolder, void* /* to not clash with a copy constructor*/)
                : ParentObjHolder_{parentObjHolder}
            {
            }

            void Init(PyObject* value) override {
                TPyObjectHolder::Init(value);
                Y_ENSURE(PyList_Append(ParentObjHolder_.Get(), value) == 0);
            };

        private:
            IPyObjectHolder& ParentObjHolder_;
        };

        class TPyDictItemHolder : public TPyObjectHolder {
        public:
            TPyDictItemHolder(IPyObjectHolder& parentObjHolder, PyObject* key)
                : ParentObjHolder_{parentObjHolder}
                , Key_{key}
            {
            }

            void Init(PyObject* value) override {
                TPyObjectHolder::Init(value);
                Y_ENSURE(PyDict_SetItem(ParentObjHolder_.Get(), Key_, value) == 0);
            };

            ~TPyDictItemHolder() override {
                Py_XDECREF(Key_);
            }

        private:
            IPyObjectHolder& ParentObjHolder_;
            PyObject* Key_;
        };

        class TParserCallbacks: public ::NJson::TJsonCallbacks {
        public:
            TParserCallbacks(bool internKeys=false, bool internVals=false)
                : TJsonCallbacks(true)
                , InternKeys_{internKeys}
                , InternVals_{internVals}
            {
                StateStack_.push_back(START);
                HolderStack_.push_back(MakeHolder<TPyObjectHolderProxy>(ResultObjHolder_));
            }

            PyObject* GetResult() {
                return ResultObjHolder_.GetNewRef();
            }

            bool OnNull() override {
                return SetValue(MakePyNoneObject());
            }

            bool OnBoolean(bool val) override {
                return SetValue(MakePyObject(val));
            }

            bool OnInteger(long long val) override {
                return SetValue(MakePyObject(val));
            }

            bool OnUInteger(unsigned long long val) override {
                return SetValue(MakePyObject(val));
            }

            bool OnString(const TStringBuf& val) override {
                return SetValue(MakePyObject(val, InternVals_));
            }

            bool OnDouble(double val) override {
                return SetValue(MakePyObject(val));
            }

            bool OnOpenArray() override {
                if (StateStack_.empty()) {
                    return false;
                }
                try {
                    switch (StateStack_.back()) {
                        case START:
                        case AFTER_MAP_KEY:
                            StateStack_.back() = IN_ARRAY; // instead of pop_back/push_back
                            HolderStack_.back()->Init(PyList_New(0));
                            Path_.push_back(0u);
                            return true;
                        case IN_ARRAY:
                            std::get<std::size_t>(Path_.back())++;
                            StateStack_.push_back(IN_ARRAY);
                            HolderStack_.push_back(MakeHolder<TPyListItemHolder>(*HolderStack_.back(), nullptr));
                            HolderStack_.back()->Init(PyList_New(0));
                            Path_.push_back(0u);
                            return true;
                        default:
                            return false;
                    }
                } catch (yexception& e) {
                    Error_ = e.what();
                    return false;
                }
            }

            bool OnCloseArray() override {
                if (StateStack_.empty()) {
                    return false;
                }
                try {
                    if (StateStack_.back() == IN_ARRAY) {
                        StateStack_.pop_back();
                        HolderStack_.pop_back();
                        Path_.pop_back();
                        RemoveMapKeyFromPath();
                        return true;
                    } else {
                        return false;
                    }
                } catch (yexception& e) {
                    Error_ = e.what();
                    return false;
                }
            }

            bool OnOpenMap() override {
                if (StateStack_.empty()) {
                    return false;
                }
                try {
                    switch (StateStack_.back()) {
                        case START:
                        case AFTER_MAP_KEY:
                            StateStack_.back() = IN_MAP; // instead of pop_back/push_back
                            HolderStack_.back()->Init(PyDict_New());
                            return true;
                        case IN_ARRAY: {
                            StateStack_.push_back(IN_MAP);
                            std::get<std::size_t>(Path_.back())++;
                            HolderStack_.push_back(MakeHolder<TPyListItemHolder>(*HolderStack_.back(), nullptr));
                            HolderStack_.back()->Init(PyDict_New());
                            return true;
                        }
                        default:
                            return false;
                    }
                } catch (yexception& e) {
                    Error_ = e.what();
                    return false;
                }
            }

            bool OnCloseMap() override {
                if (StateStack_.empty()) {
                    return false;
                }
                try {
                    if (StateStack_.back() == IN_MAP) {
                        StateStack_.pop_back();
                        HolderStack_.pop_back();
                        RemoveMapKeyFromPath();
                        return true;
                    } else {
                        return false;
                    }
                } catch (yexception& e) {
                    Error_ = e.what();
                    return false;
                }
            }

            bool OnMapKey(const TStringBuf& key) override {
                if (StateStack_.empty()) {
                    return false;
                }
                try {
                    if (StateStack_.back() == IN_MAP) {
                        HolderStack_.push_back(MakeHolder<TPyDictItemHolder>(*HolderStack_.back(), MakePyObject(key, InternKeys_)));
                        Path_.push_back(TString{key});
                        StateStack_.push_back(AFTER_MAP_KEY);
                        return true;
                    } else {
                        return false;
                    }
                } catch (yexception& e) {
                    Error_ = e.what();
                    return false;
                }
            }

            bool OnEnd() override {
                return StateStack_.empty();
            }

            void OnError(std::size_t off, TStringBuf reason) override {
                TStringStream error;
                error << (Error_ ? Error_ : reason) << " at " << GetCurrentPath();
                TJsonCallbacks::OnError(off, error.Str());
            }

        private:
            bool SetValue(PyObject* value) {
                TPyObjectPtr valKeeper{value, BORROW};
                if (StateStack_.empty()) {
                    return false;
                }
                try {
                    switch (StateStack_.back()) {
                        case START:
                        case AFTER_MAP_KEY:
                            StateStack_.pop_back();
                            HolderStack_.back()->Init(Py_NewRef(value));
                            HolderStack_.pop_back();
                            RemoveMapKeyFromPath();
                            return true;
                        case IN_ARRAY:
                            std::get<std::size_t>(Path_.back())++;
                            TPyListItemHolder(*HolderStack_.back(), nullptr).Init(Py_NewRef(value));
                            return true;
                        default:
                            return false;
                    }
                } catch (yexception& e) {
                    Error_ = e.what();
                    return false;
                }
            }

            void RemoveMapKeyFromPath() {
                if (!StateStack_.empty() && StateStack_.back() == IN_MAP) {
                    Path_.pop_back();
                }
            }

            TString GetCurrentPath() {
                TStringStream path{};
                path << "<ROOT>";
                for (const auto& p : Path_) {
                    if (std::holds_alternative<std::size_t>(p)) {
                        std::size_t idx = std::get<std::size_t>(p);
                        // To simplify code the path index is incremented before an element loading.
                        // idx == 0 - array just created (no elements)
                        // idx == 1 - the first element is loading (or has successfully loaded).
                        // If error occurred we should report about element index equals to (idx - 1).
                        // If idx = 0 and error happened, this is a syntax error (unexpected end of stream or unknown token).
                        if (idx > 0) {
                            path << '[' << (idx - 1) << ']';
                        }
                    } else {
                        TStringBuf key = std::get<TString>(p);
                        if (key) {
                            path << '.' << std::get<TString>(p);
                        }
                    }
                }
                return path.Str();
            }

        private:
            enum TState {
                START,
                AFTER_MAP_KEY,
                IN_MAP,
                IN_ARRAY
            };
            using TPathItem = std::variant<std::size_t, TString>;

            bool InternKeys_;
            bool InternVals_;
            TPyObjectHolder ResultObjHolder_{};

            TVector<TState> StateStack_{};
            TVector<TPyObjectHolderPtr> HolderStack_{};
            TString Error_{};
            TVector<TPathItem> Path_{};
        };
    }

    PyObject* LoadFromStream(PyObject* stream, const TLoaderOptions& options) {
        try {
            TPythonInputStreamWrapper input{stream};
            TParserCallbacks parserCallbacks{options.InternKeys, options.InternValues};
            ::NJson::TJsonCallbacks* callbacksPtr = &parserCallbacks;
            THolder<TRootKeyFilter> rootKeyFilter{};
            if (options.RootKeyWhiteList || options.RootKeyBlackList) {
                rootKeyFilter.Reset(new TRootKeyFilter(parserCallbacks, options.RootKeyWhiteList, options.RootKeyBlackList));
                callbacksPtr = rootKeyFilter.get();
            }
            NYa::NJson::ReadJson(input, callbacksPtr);
            return parserCallbacks.GetResult();
        } catch (TValueError& e) {
            PyErr_SetString(PyExc_ValueError, e.what());
        } catch (yexception& e) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
            }
        } catch (...) {
            if (!PyErr_Occurred()) {
                PyErr_SetString(PyExc_RuntimeError, "Unexpected error");
            }
        }
        return NULL;
    }
}
