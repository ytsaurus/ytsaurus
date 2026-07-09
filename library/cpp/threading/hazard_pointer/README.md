# **Hazard Pointer**

Эта библиотека предоставляет реализацию схемы управления памятью "hazard pointer" для безопасного управления памятью, в lock-free алгоритмах.

## Основные компоненты

### **THazardPointerDomain**
Управляет `hazard pointers` и `retired` объектами.

#### Constructors

* `THazardPointerDomain(std::size_t numRecords, std::size_t numRetires, std::size_t scanThreshold)`

    Конструктор принимает 3 аргумента:
    - **numRecords** - количество возможных `THazardPointer` для каждого потока;
    - **numRetires** - количество `retired` объектов (на самом деле их количество не ограничено; это количество бакетов во внутренней хеш-таблице) для каждого потока;
    - **scanThreshold** - порог `retired` объектов, при достижении которого запускается процесс сканирования.

    Также доступен конструктор с параметрами по умолчанию:
    ```cpp
    THazardPointerDomain(
        std::size_t numOfRecords = DefaultNumOfRecords,
        std::size_t numOfRetires = DefaultNumOfRetires,
        std::size_t scanThreshold = DefaultScanThreshold
    )
    ```
    Где значения по умолчанию:
    - `DefaultNumOfRecords` = 8
    - `DefaultNumOfRetires` = 1024
    - `DefaultScanThreshold` = 512

#### Methods

* `void AttachThread()`

    Принудительно присоединяет текущий поток к объекту `THazardPointerDomain`. При первом обращении потока к THazardPointerDomain `attach` выполняется лениво.

* `void DetachThread()`

    Отключает текущий поток от объекта `THazardPointerDomain`. Выполняется лениво, при завершении работы ранее привязанного потока.

* `std::size_t NumOfRetired()`

    Возвращает количество `retired` объектов.

* `std::size_t NumOfReclaimed()`

    Возвращает количество `reclaimed` объектов (объектов, которые уже были удалены).

* `void Retire(TValue* value, TDeleter deleter = {})`

    Метод для "удаления" **неинтрузивных** объектов (которые не наследуются от `THazardPointerObjBase`).

* `void ReclaimHazardPointers()`

    Выполняет принудительное сканирование для поиска объектов, готовых к удалению.

#### Non-member functions

* `THazardPointerDomain& GetDefaultDomain()`

    Возвращает ссылку на дефолтный `THazardPointerDomain`, у которого:
    - `numRecords` = 8
    - `numRetires` = 1024
    - `scanThreshold` = 512

* `void AttachThread(THazardPointerDomain& domain)`

    Вызывает метод `AttachThread` у переданного `domain`.

* `void DetachThread(THazardPointerDomain& domain)`

    Вызывает метод `DetachThread` у переданного `domain`.

* `void ReclaimHazardPointers(THazardPointerDomain& domain)`

    Вызывает метод `ReclaimHazardPointers` у переданного `domain`.

### **THazardPointer**

Защищает помещенный в него объект от удаления. Является ограниченным per-thread ресурсом (максимальное количество по умолчанию: 8).

#### Constructors

* `THazardPointer() noexcept`
  
    Создает пустой `hazard pointer`.

* `THazardPointer(THazardPointer&&) noexcept`

#### Methods

* `bool empty() const noexcept`

    Возвращает `true`, если `hazard pointer` пустой (не имеет соответствующей ему записи в `domain`).

* `explicit operator bool() const noexcept`

    Возвращает `true`, если `hazard pointer` не пустой.

* `TPtr Protect(const std::atomic<TPtr>& src) noexcept`

    Защищает указатель из атомарной переменной (читает ее с `std::memory_order_acquire`). Возвращает защищенный указатель. 
    - **src**: атомарная переменная, содержащая защищаемый указатель

    **Пример**:
    ```cpp
    std::atomic<TFoo*> atomicPtr {/*...*/};
    NHp::THazardPointer guard = NHp::MakeHazardPointer();

    //...

    TFoo* loadedPtr = guard.Protect(atomicPtr);

* `TPtr Protect(const std::atomic<TPtr>& src, TFunc&& func) noexcept`

    Защищает указатель, полученный путем вызова `func(src.load(std::memory_order_acquire))`. Возвращает защищенный указатель.
    - **src**: атомарная переменная с указателем
    - **func**: функция преобразования указателя (например, для работы с `tagged/marked pointers`)

    **Пример**:
    ```cpp
    std::atomic<NHp::NUtil::TMarkedPtr<TFoo>> atomicPtr {/*...*/};
    NHp::THazardPointer guard = NHp::MakeHazardPointer();

    //...

    NHp::NUtil::TMarkedPtr<TFoo> loadedPtr = guard.Protect(atomicPtr, [](const NHp::NUtil::TMarkedPtr<TFoo>& ptr){
        return ptr.Get();
    });
    ```

* `bool TryProtect(TPtr& ptr, const std::atomic<TPtr>& src) noexcept`

    Пытается защитить указатель (acquire семантика). Возвращает `true` в случае успеха.
    - **ptr**: текущее значение указателя
    - **src**: атомарная переменная с указателем

    **Пример**:
    ```cpp
    std::atomic<TFoo*> atomicPtr {/*...*/};
    NHp::THazardPointer guard = NHp::MakeHazardPointer();

    //...

    auto value = atomicPtr.load();
    while (!guard.TryProtect(value, atomicPtr)) {
        // ...
    }

* `bool TryProtect(TPtr& ptr, const std::atomic<TPtr>& src, TFunc&& func) noexcept`

    Пытается защитить указатель (acquire семантика), полученный путем вызова `func(src.load(std::memory_order_acquire))`. Возвращает `true` в случае успеха.
    - **ptr**: текущее значение указателя
    - **src**: атомарная переменная с указателем
    - **func**: функция преобразования указателя

    **Пример**:
    ```cpp
    std::atomic<NHp::NUtil::TMarkedPtr<TFoo>> atomicPtr {/*...*/};
    NHp::THazardPointer guard = NHp::MakeHazardPointer();

    //...

    auto transform = [](const NHp::NUtil::TMarkedPtr<TFoo>& ptr){
        return ptr.Get();
    };

    auto value = atomicPtr.load();
    while (!guard.TryProtect(value, atomicPtr, transform)) {
        // ...
    }

* `void ResetProtection(const TPtr ptr) noexcept`

    Защищает переданный указатель (имеет смысл только в том случае, если переданный указатель гарантированно не будет удален до завершения вызова `ResetProtection`).
    - **ptr**: защищаемый указатель

    **Пример**:
    ```cpp
    auto hp1 = MakeHazardPointer();
    auto hp2 = MakeHazardPointer();

    std::atomic<T*> atomicPtr;

    //...

    auto ptr = hp1.Protect(atomicPtr);
    hp2.ResetProtection(ptr);
    ```

* `void swap(THazardPointer& other) noexcept`

#### Non-member functions

* `THazardPointer MakeHazardPointer(THazardPointerDomain& domain = GetDefaultDomain())`

    Создает `hazard pointer` в переданном `domain`.

### **THazardPointerObjBase**

Базовый класс для объектов, поддерживающих удаление с помощью схемы управления памятью `hazard pointer`.

#### Methods

* `void Retire(TDeleter deleter, THazardPointerDomain& domain = GetDefaultDomain()) noexcept`

    Помещает объект в интрузивный список удаляемых объектов указанного `domain`.

**Пример**:
```cpp
class TMyObject: public THazardPointerObjBase<TMyObject> {
public:
    // ...
};

auto obj = new TMyObject();

obj->Retire();
```

## **Детали реализации**

### **THazardPointerDomain**

Управляет `hazard pointers` и `retired` объектами.

Ключевые структуры данных:
- `TThreadLocalList`: Управляет `thred_local` объектами. Поддерживает множественность `thread_local` объектов (Для каждого списка будет свой `thread_local` объект):

    ```cpp
    struct TFoo: public NHp::NStructs::TThreadLocalListBaseHook<TFoo> {
    };

    NHp::NStructs::TThreadLocalList<TFoo> list1;
    NHp::NStructs::TThreadLocalList<TFoo> list2;

    auto& threadLocal1 = list1.GetThreadLocal();
    auto& threadLocal2 = list2.GetThreadLocal();

    // &threadLocal1 != &threadLocal2
    ```

### **THazardThreadData**

Per-thread структура, содержащая:
- `THazardRecords`: Список активных hazard pointers
- `THazardRetires`: Список retired-объектов

### **THazardRecords**

В основе реализации `THazardRecords` лежит структура `TSharedFreeList`.

**`TSharedFreeList`** — это структура данных типа *free-list*, состоящая из двух списков: локального и глобального (thread-safe).  

- **Локальный список** используется, когда элементы добавляет поток-владелец.  
- **Глобальный список** применяется, если вставку выполняет любой другой поток.  

Таким образом, данную структуру хранения данных можно охарактеризовать как **Multi Producer Single Consumer (MPSC) free-list**.  

### **THazardRetires**

**`THazardRetires`** использует в своей основе интрузивную структуру хранения данных **`TUnorderedSet`**. Эта структура представляет собой интрузивный *hash-set*, где в качестве ключа используется адрес объекта.

### **THazardObject**

Базовый класс для объектов с поддержкой retirement через hazard pointers. Служит хуком для retired-set

### **Процесс освобождения памяти**

1. Объекты добавляются в retired-set потока
2. При достижении порога инициируется сканирование
3. Незащищенные объекты освобождаются

#### **Алгоритм сканирования**

Процесс освобождения памяти:

```cpp
void Scan(THazardThreadData& threadData) {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    // Пометка защищенных объектов во всех потоках
    for (все потоки) {
        for (все hazard-pointers) {
            if (указатель в retired-set) пометить как защищенный
        }
    }
    
    // Освобождение непомеченных объектов
    for (retired-объекты) {
        if (!защищен) освободить объект
    }
}
```

При завершении потока (или отсоединении от домена) выполняется help scan, который собирает retired-объекты из других свободных `THazardThreadData` в свой retired-set, после чего выполняет сканирование.

### **Retirement объектов**

Два подхода:
- **Intrusive**: Наследование от `THazardPointerObjBase`

    ```cpp
    class MyObj : public NHp::THazardPointerObjBase<MyObj> {};
    obj->Retire();
    ```

- **Non-intrusive**: Использование метода Retire домена
    ```cpp
    domain.Retire(ptr, deleter);
    ```
