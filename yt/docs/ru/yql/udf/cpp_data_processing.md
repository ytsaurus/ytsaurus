### Обработка данных в UDF-функциях

Для правильной работы со сложными типами данных, необходимо обработать и сохранить информацию об их структуре. Это делается на этапе формирования сигнатуры udf-функции. Пример класса, который содержит эту информацию

```cpp
struct TInputIndices {
        // В эти два поля мы занесем индексы полей в типе Struct. Эти индексы будут использоваться в дальнейшем для того, чтобы по конкретному объекту структуы получить значения ее полей.
        ui32 oneInputFieldIndex = 0;
        ui32 anotherInputFieldIndex = 0;

        static constexpr const int FieldsCount = 2;

        NYql::NUdf::TType* ResultStructType = nullptr;

        NYql::NUdf::TType* StructType() const {
            return ResultStructType;
        }

        TInputIndices(NYql::NUdf::IFunctionTypeInfoBuilder& builder) {
            auto optionalAnotherInputFieldType = builder.Optional()->Item(TDataType<TDatetime>::Id).Build();

            // После вызова Build, в oneInputFieldIndex и anotherInputFieldIndex окажется индекс соответствующих полей в структуре.
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

            // Таким образом можно можно использовать индекс, чтобы получить значение конкретного поля.
            auto oneInputFieldIValue = item.GetElement(oneInputFieldIndex);
            auto anotherInputFieldIValue = item.GetElement(anotherInputFieldIndex);

            ...

            return someStruct;
        }
    };
```

Для формирования типа входных данных понадобится `IFunctionTypeInfoBuilder`.
Данный интерфейс поддерживает генерацию сложных типов, таких как структуры, списки, словари и т.д.
Допустим на вход подается структура, содержащая два поля - `"oneInputField"` типа `Double` и `"anotherInputField"` типа `TDatetime?`. С помощью билдера `IFunctionTypeInfoBuilder`, создается optional-тип для второго поля и создается тип структуры, который заполняется нужными полями.

Объявление типа структуры просиходит с помощью билдера через вызов метода `Struct(num_of_fields)`. Под капотом, структура в YQL UDF представляется как обычный массив из значений полей.

Добавление полей осуществляется с помощью метода `AddField` возвращаемого интерфейса для работы со структурами.
После выполнения метода Build(), переменные `oneInputFieldIndex` и `anotherInputFieldIndex` будут содержать индексы, по которым во входном массиве данных буду расположены данные конкретного поля. С их помощью можно получить значение конкретного поля из входного массива данных, как это сделано в методе `UnpackUnboxedValue` в примере выше.

Далее этот класс можно использовать в методе `Run` udf-функции для распаковки
входных данных с помощью метода `UnpackUnboxedValue`.

Для упаковки результатов работы вашей udf можно использовать такие же классы для
хранения информации уже о выходных данных. Например:

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
        // Обратите внимание на то, что происходит в этом месте. Здесь ДВА выходных параметра: result - это то что вам надо будет вернуть. А вот в elemItems будет записан указатель на массив, в который вам нужно занести значения полей.
        TUnboxedValue* elemItems = nullptr;
        auto result = valueBuilder->NewArray(FieldsCount, elemItems);

        // Заполняем значения для полей
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

Формирование типа выходной структуры данных ничем не отличается от формирования типа входной структуры - так же нужно объявить поля структуры и сохранить их индексы для последующего заполнения выходного массива данных. В примере выше эта структура будет состоять из трех поле: одно поле будет иметь примитивный числовой тип, другое представляет собой строку, а третье - список. Для создания типа списка, нужно воспользоваться методом `List()` интерфейса `NYql::NUdf::IFunctionTypeInfoBuilder`. Этот метод вернет указатель на интерфейс, позволяющий создать тип списка. Указание типа элементов списка происходит через метод `Item(type)`. Таким образом, кроме списков можно создавать словари, структуры, кортежи и т.д.

Формирование выходного массива данных происходит с помощью `NYql::NUdf::IValueBuilder` и его метода `NewArray`, который принимает на вход количество выходных элкметов + ссылку на указатель, а возвращает TUnboxedValue описывающий объект целиком + заполняет значением переданный указатель, с помощью которого будет происходить дальнейшее заполнение массива.

Данный метод возвращает `NYql::NUdf::TUnboxedValue`, который нужно вернуть после заполнения выходного массива необходимыми значениями. Важно понимать, что для заполнения выходного массива нужно использовать именно тот указатель,
который метод заполнил, а не то, что он вернул. При заполнении массива конкретным значением для конкретного поля, нужно использовать те индексы, которые ассоциированны с данным полем на этапе формирования типа выходных данных.

Если возвращаемое значение является примитивным числовым типом, то это значение можно просто обернуть в `TUnboxedValuePod` и положить это значение по соответствующему индексу.

Если возвращаемое значение является строкой, то необходимо воспользоваться методом `NewString` интерфейса `NYql::NUdf::IValueBuilder`, в который передать нужную строку.

Если возвращаемое значение является, например, списком, то необходимо создать массив значений `TUnboxedValue`, заполнить его, а затем передать его размер и указатель в метод `NewList` интерфейса `NYql::NUdf::IValueBuilder`. Данный метод вернет `TUnboxedValue`, который можно положить в выходной массив по соответствующему данному полю индексу.
