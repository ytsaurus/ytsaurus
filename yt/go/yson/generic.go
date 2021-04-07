package yson

// ValueWithAttrs is generic representation of YSON node with attached attributes.
type ValueWithAttrs struct {
	Attrs map[string]interface{} `yson:",attrs"`
	Value interface{}            `yson:",value"`
}

// AttrsOf is a helper function for working with ValueWithAttrs.
// If value is pointer to ValueWithAttrs, returns attributes.
// Otherwise returns nil.
func AttrsOf(value interface{}) map[string]interface{} {
	if v, ok := value.(*ValueWithAttrs); ok {
		return v.Attrs
	}
	return nil
}

// ValueOf is a helper function for working with ValueWithAttrs.
// If value is pointer to ValueWithAttrs, return inner value.
// Otherwise returns value directly.
func ValueOf(value interface{}) interface{} {
	if v, ok := value.(*ValueWithAttrs); ok {
		return v.Value
	}
	return value
}

func decodeGeneric(r *Reader, v *interface{}) error {
	e, err := r.Next(false)
	if err != nil {
		return err
	}

	if e == EventBeginAttrs {
		attrs := make(map[string]interface{})

		var valueWithAttrs ValueWithAttrs
		*v = &valueWithAttrs
		v = &valueWithAttrs.Value
		valueWithAttrs.Attrs = attrs

		for {
			ok, err := r.NextKey()
			if err != nil {
				return err
			}
			if !ok {
				break
			}

			attrName := r.String()
			var attrValue interface{}
			if err = decodeGeneric(r, &attrValue); err != nil {
				return err
			}

			attrs[attrName] = attrValue
		}

		e, err = r.Next(false)
		if err != nil {
			return err
		}
		if e != EventEndAttrs {
			panic("invalid decoder state")
		}

		e, err = r.Next(false)
		if err != nil {
			return err
		}
	}

	switch e {
	case EventLiteral:
		switch r.currentType {
		case TypeFloat64:
			*v = r.currentFloat
		case TypeString:
			*v = r.String()
		case TypeBool:
			*v = r.currentBool
		case TypeInt64:
			*v = r.currentInt
		case TypeUint64:
			*v = r.currentUint
		case TypeEntity:
		default:
			panic("missed type")
		}
	case EventBeginMap:
		m := make(map[string]interface{})
		*v = m
		for {
			if ok, err := r.NextKey(); err != nil {
				return err
			} else if !ok {
				break
			}

			mapKey := r.String()
			var mapValue interface{}
			if err = decodeGeneric(r, &mapValue); err != nil {
				return err
			}

			m[mapKey] = mapValue
		}

		e, err = r.Next(false)
		if err != nil {
			return err
		}

		if e != EventEndMap {
			panic("invalid decoder state")
		}
	case EventBeginList:
		a := []interface{}{}

		for {
			ok, err := r.NextListItem()
			if err != nil {
				return err
			}
			if !ok {
				break
			}

			var e interface{}
			if err = decodeGeneric(r, &e); err != nil {
				return err
			}
			a = append(a, e)
		}
		*v = a

		e, err = r.Next(false)
		if err != nil {
			return err
		}

		if e != EventEndList {
			panic("invalid decoder state")
		}
	default:
		panic("invalid decoder state")
	}

	return nil
}
