package yson

// ValueWithAttrs is a generic representation of YSON node with attached attributes.
type ValueWithAttrs struct {
	Attrs map[string]any `yson:",attrs"`
	Value any            `yson:",value"`
}

// AttrsOf is a helper function to work with ValueWithAttrs.
// If value is a pointer to ValueWithAttrs, returns attributes.
// Otherwise returns nil.
func AttrsOf(value any) map[string]any {
	if v, ok := value.(*ValueWithAttrs); ok {
		return v.Attrs
	}
	return nil
}

// ValueOf is a helper function to work with ValueWithAttrs.
// If value is a pointer to ValueWithAttrs, returns inner value.
// Otherwise returns value directly.
func ValueOf(value any) any {
	if v, ok := value.(*ValueWithAttrs); ok {
		return v.Value
	}
	return value
}

func decodeGeneric(r *Reader, v *any) error {
	e, err := r.Next(false)
	if err != nil {
		return err
	}

	if e == EventBeginAttrs {
		attrs := make(map[string]any)

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
			var attrValue any
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
		m := make(map[string]any)
		*v = m
		for {
			if ok, err := r.NextKey(); err != nil {
				return err
			} else if !ok {
				break
			}

			mapKey := r.String()
			var mapValue any
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
		a := []any{}

		for {
			ok, err := r.NextListItem()
			if err != nil {
				return err
			}
			if !ok {
				break
			}

			var e any
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

func decodeMap(r *Reader, v *map[string]any) error {
	e, err := r.Next(true)
	if err != nil {
		return err
	}

	switch e {
	case EventLiteral:
		if r.currentType != TypeEntity {
			panic("invalid decoder state")
		}
	case EventBeginMap:
		m := make(map[string]any)
		*v = m
		for {
			if ok, err := r.NextKey(); err != nil {
				return err
			} else if !ok {
				break
			}

			mapKey := r.String()
			var mapValue any
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
	default:
		panic("invalid decoder state")
	}

	return nil
}
