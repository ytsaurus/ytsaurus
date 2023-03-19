// Package ytwalk implements cypress traversal.
package ytwalk

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

// ErrSkipSubtree is sentinel value returned from OnNode.
var ErrSkipSubtree = errors.New("skip subtree")

type Walk struct {
	// Root is cypress path where walk starts.
	Root ypath.Path

	// Attributes are requested for each node.
	Attributes []string

	// Node is optional pointer to type that will hold deserialized cypress node.
	Node interface{}

	// OnNode is invoked for each node during traversal.
	OnNode func(path ypath.Path, node interface{}) error

	// RespectOpaque defines whether we stop at opaque nodes (true) or not (false).
	RespectOpaque bool
}

type treeOrValue struct {
	Children map[string]tree
	Value    interface{}
}

func (s *treeOrValue) UnmarshalYSON(r *yson.Reader) error {
	e, err := r.Next(false)
	if err != nil {
		return err
	}
	r.Undo(e)

	d := &yson.Decoder{R: r}
	if e == yson.EventBeginMap {
		return d.Decode(&s.Children)
	} else {
		return d.Decode(&s.Value)
	}
}

type tree struct {
	Attributes map[string]interface{} `yson:",attrs"`
	Value      treeOrValue            `yson:",value"`
}

func Do(ctx context.Context, yc yt.Client, w *Walk) error {
	opts := &yt.GetNodeOptions{
		Attributes: append([]string{"opaque"}, w.Attributes...),
	}

	var t tree
	if err := yc.GetNode(ctx, w.Root, &t, opts); err != nil {
		return fmt.Errorf("walk %q failed: %w", w.Root, err)
	}

	var walk func(path ypath.Path, t tree) error
	walk = func(path ypath.Path, t tree) error {
		var node interface{}
		if w.Node != nil {
			node = reflect.New(reflect.TypeOf(w.Node).Elem()).Interface()

			ysonNode, _ := yson.Marshal(yson.ValueWithAttrs{Attrs: t.Attributes, Value: t.Value.Value})
			if err := yson.Unmarshal(ysonNode, node); err != nil {
				return fmt.Errorf("walk %q failed: %w", path, err)
			}
		}

		if !w.RespectOpaque && t.Attributes["opaque"] == true && path != w.Root {
			subwalk := *w
			subwalk.Root = path
			return Do(ctx, yc, &subwalk)
		}

		err := w.OnNode(path, node)
		if err == ErrSkipSubtree {
			return nil
		} else if err != nil {
			return err
		}

		for name, child := range t.Value.Children {
			if err := walk(path.Child(name), child); err != nil {
				return err
			}
		}

		return nil
	}

	return walk(w.Root, t)
}
