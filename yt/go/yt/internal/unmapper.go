package internal

import (
	"encoding/json"
	"fmt"

	"go.ytsaurus.tech/yt/go/yson"
)

// unmapper decodes value of single key inside YSON map.
type unmapper struct {
	value any
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

// jsonUnmapper decodes value of single key inside JSON map.
type jsonUnmapper struct {
	value any
	key   string
}

func (u *jsonUnmapper) UnmarshalJSON(data []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("JSON value is not a map: %w", err)
	}

	rawValue, exists := m[u.key]
	if !exists {
		return fmt.Errorf("JSON map is missing key %q", u.key)
	}

	if err := json.Unmarshal(rawValue, u.value); err != nil {
		return fmt.Errorf("failed to unmarshal JSON value for key %q: %w", u.key, err)
	}

	return nil
}
