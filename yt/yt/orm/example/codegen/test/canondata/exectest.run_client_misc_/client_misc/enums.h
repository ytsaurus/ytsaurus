// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NOrm::NExample::NClient::NApi {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EObjectType,
    ((Null)                                              (0))
    ((Book)                                              (1))
    ((Author)                                            (2))
    ((Publisher)                                         (3))
    ((Editor)                                            (4))
    ((Illustrator)                                       (5))
    ((Typographer)                                       (6))
    ((Genre)                                             (7))
    ((User)                                              (9))
    ((Group)                                             (10))
    ((Hitchhiker)                                        (42))
    ((Nexus)                                             (100))
    ((MotherShip)                                        (101))
    ((Interceptor)                                       (102))
    ((Executor)                                          (103))
    ((NirvanaDMProcessInstance)                          (104))
    ((ManualId)                                          (200))
    ((RandomId)                                          (201))
    ((TimestampId)                                       (202))
    ((BufferedTimestampId)                               (203))
    ((IndexedIncrementId)                                (204))
    ((MultipolicyId)                                     (206))
    ((Employer)                                          (207))
    ((NestedColumns)                                     (208))
    ((Schema)                                            (256))
    ((WatchLogConsumer)                                  (257))
    ((Semaphore)                                         (258))
    ((SemaphoreSet)                                      (259))
    ((Cat)                                               (260))
);

DEFINE_ENUM(EEyeColor,
    ((Unknown) (0))
    ((Hazel) (1))
    ((Green) (2))
    ((Blue) (3))
    ((Orange) (4))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EHealthCondition,
    ((Unknown) (0))
    ((Ill) (1))
    ((Healthy) (2))
);

DEFINE_ENUM(EMood,
    ((Unknown) (0))
    ((Sleepy) (1))
    ((Angry) (2))
    ((Scared) (3))
    ((Playful) (4))
    ((Friendly) (5))
);

////////////////////////////////////////////////////////////////////////////////

TStringBuf FormatType(EObjectType v);

bool TryParseType(TStringBuf s, EObjectType* v);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NApi
