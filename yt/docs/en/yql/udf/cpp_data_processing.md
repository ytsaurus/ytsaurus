### Data processing in UDFs

To properly use composite data types, you need to process and store information about their structure. This is done at the UDF signature generation stage. Example of a class that contains this information

```cpp
struct TInputIndices {
        // These two fields will contain indexes of fields in the Struct type. Using these indexes, we will be able to obtain the values of the structure fields of a specific object of that structure.
        ui32 oneInputFieldIndex = 0;
        ui32 anotherInputFieldIndex = 0;

        static constexpr const int FieldsCount = 2;

        NYql::NUdf::TType* ResultStructType = nullptr;

        NYql::NUdf::TType* StructType() const {
            return ResultStructType;
        }

        TInputIndices(NYql::NUdf::IFunctionTypeInfoBuilder& builder) {
            auto optionalAnotherInputFieldType = builder.Optional()->Item(TDataType<TDatetime>::Id).Build();

            // After calling Build, oneInputFieldIndex and anotherInputFieldIndex will contain the index of the corresponding structure fields.
            ResultStructType = builder.Struct(FieldsCount)
                                   ->
                               AddField<double>("oneInputFieldName", &oneInputFieldIndex)
                                   .
                               AddField("anotherInputFieldName", optionalAnotherInputFieldType, &anotherInputFieldIndex)
                                   .
                               Build();
        }

        TSomeStruct UnpackUnboxedValue(const NYql::NUdf::TUnboxedValue& item) const {
            TSomeStruct someStruct;

            // We can get the value of a particular field using the index.
            auto oneInputFieldIValue = item.GetElement(oneInputFieldIndex);
            auto anotherInputFieldIValue = item.GetElement(anotherInputFieldIndex);

            ...

            return someStruct;
        }
    };
```

You will need `IFunctionTypeInfoBuilder` to generate the input data type.
This interface supports generation of composite types, such as structures, lists, or maps.
Suppose that we pass a structure containing two fields (`"oneInputField"` of the `Double` type and `"anotherInputField"` of the `TDatetime?` type) as input. Using `IFunctionTypeInfoBuilder`, we create an optional type for the second field and a structure type filled with the required fields.

The structure type is declared using the builder by calling the `Struct(num_of_fields)` method. Internally, the structure in a YQL UDF is a regular array of field values.

You can add fields using the `AddField` method of the returned interface for working with structures.
After executing the Build() method, the `oneInputFieldIndex` and `anotherInputFieldIndex` variables will contain the indexes by which the data of a particular field will be stored in the input data array. You can use them to retrieve the value of a particular field from the input data array as it was done in the `UnpackUnboxedValue` method in the example above.

Then you can use this class in the `Run` method of the UDF to unpack
input data using the `UnpackUnboxedValue` method.

To pack the results of your UDF, you can use classes for
storing output data details. For example:

```cpp
struct TOutputIndices {
    ui32 firstOutputFieldIndex = 0;
    ui32 secondOutputFieldIndex = 0;
    ui32 thirdOutputFieldIndex = 0;

    static constexpr const int FieldsCount = 3;

    NYql::NUdf::TType* ResultStructType = nullptr;

    NYql::NUdf::TType* StructType() const {
        return ResultStructType;
    }

    TOutputIndices(NYql::NUdf::IFunctionTypeInfoBuilder& builder) {
        auto thirdOutputFieldType = builder.List()->Item(TDataType<ui32>::Id).Build()

        ResultStructType =
            builder.Struct(FieldsCount)
                  ->
            AddField<ui32>("firstOutputFieldName", &firstOutputFieldIndex)
                  .
            AddField<char*>("secondOutputFieldName", &secondOutputFieldIndex)
                  .
            AddField("thirdOutputFieldName", thirdOutputFieldType, &thirdOutputFieldIndex);
                  .
            Build();
    }

    NYql::NUdf::TUnboxedValue PackUnboxedValue(const NYql::NUdf::IValueBuilder* valueBuilder, ui32 valueForFirstField, const TString& valueForSecondField, std::vector<ui32> valuesForThirdField) {
        // Pay attention to what happens here. There are TWO output parameters: result is what you need to return, while elemItems contains a pointer to the array where you need to enter the field values.
        TUnboxedValue* elemItems = nullptr;
        auto result = valueBuilder->NewArray(FieldsCount, elemItems);

        // Entering field values
        elemItems[firstOutputFieldIndex] = TUnboxedValuePod(valueForFirstField);
        elemItems[secondOutputFieldIndex] = valueBuilder->NewString(valueForSecondField);

        std::vector<TUnboxedValue> thirdFieldItems;
        thirdFieldItems.reserve(valuesForThirdField.size());

        for (const auto& value : valuesForThirdField) {
            TUnboxedValue yqlValue = TUnboxedValuePod(value);

            if (yqlValue) {
                thirdFieldItems.push_back(std::move(yqlValue));
            }
        }

        auto thirdFieldValueHandler = valueBuilder->NewList(thirdFieldItems.data(),
                                                            thirdFieldItems.size());

        elemItems[thirdOutputFieldIndex] = thirdFieldValueHandler;

        return result;
    }
};
```

Generating the output data structure type is no different from generating the input data structure type: you need to declare the structure fields and save their indexes to then fill the output data array. In the example above, the structure consists of three fields: the first field is a primitive numeric type, the second one is a string, and the third one is a list. To create a list type, use the `List()` method of the `NYql::NUdf::IFunctionTypeInfoBuilder` interface. This method returns a pointer to the interface that you can use to create a list type. Specify the type of list items using the `Item(type)` method. Thus, in addition to lists, you can create maps, structures, tuples, and more.

The output data array is generated using `NYql::NUdf::IValueBuilder` and its `NewArray` method, which accepts a set of output elements and a reference to the pointer. The method returns TUnboxedValue describing the entire object and adds a value to the passed pointer, which in turn helps fill the array.

This method returns `NYql::NUdf::TUnboxedValue` that should be returned after filling the output array with the necessary values. Keep in mind that, in order to fill the output array, you should use the pointer
that the method fills, not the one it returns. When filling the array with a specific value for a specific field, use the indexes associated with that field at the stage of generating the output data type.

If the returned value is a primitive numeric type, you can simply wrap it in `TUnboxedValuePod` and place it according to the appropriate index.

If the returned value is a string, use the `NewString` method of the `NYql::NUdf::IValueBuilder` interface where you need to pass the string to.

If the returned value is, for example, a list, create a `TUnboxedValue` value array, fill it, and then pass its size and pointer to the `NewList` method of the `NYql::NUdf::IValueBuilder` interface. This method returns `TUnboxedValue`, which you can then place in the output array according to the index that corresponds to that field.
