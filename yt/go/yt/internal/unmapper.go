package internal

import (
	"fmt"

	"go.ytsaurus.tech/yt/go/yson"
)

// unmapper decodes value of single key inside YSON map.
type unmapper struct {
	value interface{}
	key   string
}

func (u *unmapper) UnmarshalYSON(r *yson.Reader) error {
	if event, err := r.Next(true); err != nil {
		return err
	} else if event != yson.EventBeginMap {
		return fmt.Errorf("YSON value is not a map")
	}

	var keyFound bool
	for {
		if ok, err := r.NextKey(); err != nil {
			return err
		} else if !ok {
			break
		}

		if r.String() == u.key {
			keyFound = true
			if err := (&yson.Decoder{R: r}).Decode(u.value); err != nil {
				return err
			}
		} else {
			if _, err := r.NextRawValue(); err != nil {
				return err
			}
		}
	}

	if event, err := r.Next(false); err != nil {
		return err
	} else if event != yson.EventEndMap {
		panic("inconsistent reader state")
	}

	if !keyFound {
		return fmt.Errorf("YSON map is missing key %q", u.key)
	}

	return nil
}
