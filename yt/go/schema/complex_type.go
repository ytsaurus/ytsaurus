package schema

import (
	"fmt"

	"go.ytsaurus.tech/yt/go/yson"
)

type ComplexType interface {
	isType()
}

func (Type) isType()     {}
func (Decimal) isType()  {}
func (Optional) isType() {}
func (List) isType()     {}
func (Struct) isType()   {}
func (Tuple) isType()    {}
func (Variant) isType()  {}
func (Dict) isType()     {}
func (Tagged) isType()   {}

func decodeComplexType(r *yson.Reader, tt interface{}) error {
	t := tt.(*ComplexType)

	e, err := r.Next(true)
	if err != nil {
		return err
	}

	if r.Type() == yson.TypeString {
		st := Type(r.String())
		if st == typeBool {
			st = TypeBoolean
		} else if st == typeYSON {
			st = TypeAny
		}
		*t = st
		return nil
	}

	r.Undo(e)
	v, err := r.NextRawValue()
	if err != nil {
		return err
	}

	var typeName struct {
		TypeName TypeName `yson:"type_name"`
	}
	if err := yson.Unmarshal(v, &typeName); err != nil {
		return err
	}

	switch typeName.TypeName {
	case TypeNameDecimal:
		var d Decimal
		if err := yson.Unmarshal(v, &d); err != nil {
			return err
		}
		*t = d

	case TypeNameOptional:
		var o Optional
		if err := yson.Unmarshal(v, &o); err != nil {
			return err
		}
		*t = o

	case TypeNameList:
		var l List
		if err := yson.Unmarshal(v, &l); err != nil {
			return err
		}
		*t = l

	case TypeNameStruct:
		var s Struct
		if err := yson.Unmarshal(v, &s); err != nil {
			return err
		}
		*t = s

	case TypeNameTuple:
		var tu Tuple
		if err := yson.Unmarshal(v, &tu); err != nil {
			return err
		}
		*t = tu

	case TypeNameVariant:
		var va Variant
		if err := yson.Unmarshal(v, &va); err != nil {
			return err
		}
		*t = va

	case TypeNameDict:
		var d Dict
		if err := yson.Unmarshal(v, &d); err != nil {
			return err
		}
		*t = d

	case TypeNameTagged:
		var ta Tagged
		if err := yson.Unmarshal(v, &ta); err != nil {
			return err
		}
		*t = ta

	default:
		return fmt.Errorf("unsupported type name %q", typeName.TypeName)
	}

	return nil
}

func init() {
	yson.RegisterInterfaceDecoder((*ComplexType)(nil), decodeComplexType)
}

type (
	Decimal struct {
		Precision int `yson:"precision"`
		Scale     int `yson:"scale"`
	}

	Optional struct {
		Item ComplexType `yson:"item"`
	}

	List struct {
		Item ComplexType `yson:"item"`
	}

	StructMember struct {
		Name string      `yson:"name"`
		Type ComplexType `yson:"type"`
	}

	Struct struct {
		Members []StructMember `yson:"members"`
	}

	TupleElement struct {
		Type ComplexType `yson:"type"`
	}

	Tuple struct {
		Elements []TupleElement `yson:"elements"`
	}

	Variant struct {
		Members  []StructMember `yson:"members"`
		Elements []TupleElement `yson:"elements"`
	}

	Dict struct {
		Key   ComplexType `yson:"key"`
		Value ComplexType `yson:"value"`
	}

	Tagged struct {
		Tag  string      `yson:"tag"`
		Item ComplexType `yson:"item"`
	}
)

type TypeName string

const (
	TypeNameDecimal  TypeName = "decimal"
	TypeNameOptional TypeName = "optional"
	TypeNameList     TypeName = "list"
	TypeNameStruct   TypeName = "struct"
	TypeNameTuple    TypeName = "tuple"
	TypeNameVariant  TypeName = "variant"
	TypeNameDict     TypeName = "dict"
	TypeNameTagged   TypeName = "tagged"
)

type Any interface{}

func fixV3type(t ComplexType) ComplexType {
	switch t {
	case TypeBoolean:
		return typeBool
	case TypeAny:
		return typeYSON
	default:
		return t
	}
}

func marshalTypeV3(w *yson.Writer, t ComplexType) {
	w.Any(fixV3type(t))
}

func (m StructMember) MarshalYSON(w *yson.Writer) error {
	type member StructMember

	mm := member(m)
	mm.Type = fixV3type(mm.Type)
	w.Any(mm)
	return w.Err()
}

func (e TupleElement) MarshalYSON(w *yson.Writer) error {
	type element TupleElement

	ee := element(e)
	ee.Type = fixV3type(ee.Type)
	w.Any(ee)
	return w.Err()
}

func (d Decimal) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameDecimal))

	w.MapKeyString("precision")
	w.Int64(int64(d.Precision))

	w.MapKeyString("scale")
	w.Int64(int64(d.Scale))

	w.EndMap()
	return w.Err()
}

func (o Optional) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameOptional))

	w.MapKeyString("item")
	marshalTypeV3(w, o.Item)

	w.EndMap()
	return w.Err()
}

func (l List) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameList))

	w.MapKeyString("item")
	marshalTypeV3(w, l.Item)

	w.EndMap()
	return w.Err()
}

func (s Struct) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameStruct))

	w.MapKeyString("members")
	w.Any(s.Members)

	w.EndMap()
	return w.Err()
}

func (t Tuple) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameTuple))

	w.MapKeyString("elements")
	w.Any(t.Elements)

	w.EndMap()
	return w.Err()
}

func (v Variant) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameVariant))

	if len(v.Members) != 0 {
		w.MapKeyString("members")
		w.Any(v.Members)
	} else {
		w.MapKeyString("elements")
		w.Any(v.Elements)
	}

	w.EndMap()
	return w.Err()
}

func (d Dict) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameDict))

	w.MapKeyString("key")
	marshalTypeV3(w, d.Key)

	w.MapKeyString("value")
	marshalTypeV3(w, d.Value)

	w.EndMap()
	return w.Err()
}

func (t Tagged) MarshalYSON(w *yson.Writer) error {
	w.BeginMap()

	w.MapKeyString("type_name")
	w.String(string(TypeNameTagged))

	w.MapKeyString("tag")
	w.Any(t.Tag)

	w.MapKeyString("item")
	marshalTypeV3(w, t.Item)

	w.EndMap()
	return w.Err()
}
