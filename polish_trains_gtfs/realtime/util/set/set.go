// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package set

import (
	"iter"
	"maps"
)

type Set[T comparable] map[T]struct{}

func (s Set[T]) Len() int {
	return len(s)
}

func (s Set[T]) Has(item T) bool {
	_, has := s[item]
	return has
}

func (s Set[T]) Add(item T) {
	s[item] = struct{}{}
}

func (s Set[T]) Discard(item T) {
	delete(s, item)
}

func (s Set[T]) Iter() iter.Seq[T] {
	return maps.Keys(s)
}
