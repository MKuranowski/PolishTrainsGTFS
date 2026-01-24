// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package mcsv

import (
	"encoding/csv"
	"errors"
	"io"
	"iter"
	"slices"
)

type Reader struct {
	r      *csv.Reader
	header []string
	record map[string]string
	err    error
}

func NewReader(r io.Reader) *Reader {
	o := &Reader{r: csv.NewReader(r)}
	o.r.ReuseRecord = true
	return o
}

func (r *Reader) readHeader() {
	var row []string
	row, r.err = r.r.Read()
	if r.err != nil {
		return
	}

	r.header = slices.Clone(row)
}

func (r *Reader) next() {
	if r.header == nil {
		r.readHeader()
		if r.err != nil {
			return
		}
	}

	if r.record == nil {
		r.record = make(map[string]string, len(r.header))
	}

	var row []string
	row, r.err = r.r.Read()
	if r.err != nil {
		return
	}

	for i, key := range r.header {
		r.record[key] = row[i]
	}
}

func (r *Reader) Read() (map[string]string, error) {
	r.next()
	if r.err != nil {
		return nil, r.err
	}
	return r.record, nil
}

func (r *Reader) Iter() iter.Seq[map[string]string] {
	return func(yield func(map[string]string) bool) {
		for {
			r.next()
			if r.err != nil || !yield(r.record) {
				return
			}
		}
	}
}

func (r *Reader) Err() error {
	if errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}

func (r *Reader) Line() int {
	line, _ := r.r.FieldPos(0)
	return line
}
