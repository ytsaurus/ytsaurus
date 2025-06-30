### Working with YQL-Stream in a C++ UDF

YQL-Stream is a single-pass iterator. In YQL, it can be obtained at the ```PROCESS``` or ```REDUCE``` output (see the YQL documentation).

In C++, YQL-Stream is a ```TBoxedValue``` subclass with the overridden ```Fetch()``` method.

```cpp
class TMyStreamProvider: public TBoxedValue {
public:
    TMyStreamProvider(...) {
        ...
    }

    TMyStreamProvider() = delete;
    TMyStreamProvider(const TMyStreamProvider&) = delete;
    virtual ~TMyStreamProvider() = default;
    void operator=(const TMyStreamProvider&) = delete;
    TMyStreamProvider(TMyStreamProvider&& other) = default;
    TMyStreamProvider& operator=(TMyStreamProvider&& other) = default;

    EFetchStatus Fetch(TUnboxedValue& result) override;
};
```

```Fetch()``` can return three statuses:
* ```EFetchStatus::Ok```

  Indicates successful writing of a value to the result.

* ```EFetchStatus::Finish```

  Indicates the end of the stream, the result remains unchanged.

* ```EFetchStatus::Yield```

  Special status. If a UDF has no YQL-Stream at the input, your stream will never generate this status. If the ```EFetchStatus::Yield``` status appears when reading the input stream, you can write 0 or more values to the output stream but the ```EFetchStatus::Yield``` status should still be generated and returned.  

Remember that after the UDF returns a stream in C++ code, whatever was in the ```Run()``` body is destroyed by the destructor. So you need to pass everything through the constructor to the Stream class to make it the owner of all resources.
