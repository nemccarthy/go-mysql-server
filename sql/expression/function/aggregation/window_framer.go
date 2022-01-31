// Copyright 2022 DoltHub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"errors"
	"io"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

//go:generate optgen -out window_framer.og.go -pkg aggregation framer window_framer.go

var ErrPartitionNotSet = errors.New("attempted to general a window frame interval before framer partition was set")
var ErrRangeIntervalTypeMismatch = errors.New("range bound type must match the order by expression type")

var _ sql.WindowFramer = (*RowFramer)(nil)
var _ sql.WindowFramer = (*PartitionFramer)(nil)
var _ sql.WindowFramer = (*GroupByFramer)(nil)

// NewUnboundedPrecedingToCurrentRowFramer generates sql.WindowInterval
// from the first row in a partition to the current row.
//
// Ex: partition = [0, 1, 2, 3, 4, 5]
// =>
// frames: {0,0},   {0,1}, {0,2},   {0,3},     {0,4},     {0,5}
// rows:   [0],     [0,1], [0,1,2], [1,2,3,4], [1,2,3,4], [1,2,3,4,5]
func NewUnboundedPrecedingToCurrentRowFramer() *RowFramer {
	return &RowFramer{
		unboundedPreceding: true,
		followingOffset:    0,
		frameEnd:           -1,
		frameStart:         -1,
		partitionStart:     -1,
		partitionEnd:       -1,
	}
}

// NewNPrecedingToCurrentRowFramer generates sql.WindowInterval
// in the active partitions using an index offset.
//
// Ex: precedingOffset = 3; partition = [0, 1, 2, 3, 4, 5]
// =>
// frames: {0,0},   {0,1}, {0,2},   {0,3},   {1,4},   {2,5}
// rows:   [0],     [0,1], [0,1,2], [1,2,3], [2,3,4], [3,4,5]
func NewNPrecedingToCurrentRowFramer(n int) *RowFramer {
	return &RowFramer{
		precedingOffset: n,
		followingOffset: 0,
		frameEnd:        -1,
		frameStart:      -1,
		partitionStart:  -1,
		partitionEnd:    -1,
	}
}

type RowFramer struct {
	idx                          int
	partitionStart, partitionEnd int
	frameStart, frameEnd         int
	partitionSet                 bool

	followingOffset, precedingOffset       int
	unboundedPreceding, unboundedFollowing bool
}

func (f *RowFramer) Close() {
	return
}

func (f *RowFramer) NewFramer(interval sql.WindowInterval) (sql.WindowFramer, error) {
	return &RowFramer{
		idx:            interval.Start,
		partitionStart: interval.Start,
		partitionEnd:   interval.End,
		frameStart:     -1,
		frameEnd:       -1,
		partitionSet:   true,
		// pass through parent state
		unboundedPreceding: f.unboundedPreceding,
		unboundedFollowing: f.unboundedFollowing,
		followingOffset:    f.followingOffset,
		precedingOffset:    f.precedingOffset,
	}, nil
}

func (f *RowFramer) Next(ctx *sql.Context, buffer sql.WindowBuffer) (sql.WindowInterval, error) {
	if f.idx != 0 && f.idx >= f.partitionEnd || !f.partitionSet {
		return sql.WindowInterval{}, io.EOF
	}

	newStart := f.idx - f.precedingOffset
	if f.unboundedPreceding || newStart < f.partitionStart {
		newStart = f.partitionStart
	}

	newEnd := f.idx + f.followingOffset + 1
	if f.unboundedFollowing || newEnd > f.partitionEnd {
		newEnd = f.partitionEnd
	}

	f.frameStart = newStart
	f.frameEnd = newEnd

	f.idx++
	return f.Interval()
}

func (f *RowFramer) FirstIdx() int {
	return f.frameEnd
}

func (f *RowFramer) LastIdx() int {
	return f.frameStart
}

func (f *RowFramer) Interval() (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, ErrPartitionNotSet
	}
	return sql.WindowInterval{Start: f.frameStart, End: f.frameEnd}, nil
}

func (f *RowFramer) SlidingInterval(ctx sql.Context) (sql.WindowInterval, sql.WindowInterval, sql.WindowInterval) {
	panic("implement me")
}

type PartitionFramer struct {
	idx                          int
	partitionStart, partitionEnd int

	followOffset, precOffset int
	frameStart, frameEnd     int
	partitionSet             bool
}

func NewPartitionFramer() *PartitionFramer {
	return &PartitionFramer{
		idx:            -1,
		frameEnd:       -1,
		frameStart:     -1,
		partitionStart: -1,
		partitionEnd:   -1,
	}
}

func (f *PartitionFramer) NewFramer(interval sql.WindowInterval) (sql.WindowFramer, error) {
	return &PartitionFramer{
		idx:            interval.Start,
		frameEnd:       interval.End,
		frameStart:     interval.Start,
		partitionStart: interval.Start,
		partitionEnd:   interval.End,
		partitionSet:   true,
	}, nil
}

func (f *PartitionFramer) Next(ctx *sql.Context, buffer sql.WindowBuffer) (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, io.EOF
	}
	if f.idx == 0 || (0 < f.idx && f.idx < f.partitionEnd) {
		f.idx++
		return f.Interval()
	}
	return sql.WindowInterval{}, io.EOF
}

func (f *PartitionFramer) FirstIdx() int {
	return f.frameStart
}

func (f *PartitionFramer) LastIdx() int {
	return f.frameEnd
}

func (f *PartitionFramer) Interval() (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, ErrPartitionNotSet
	}
	return sql.WindowInterval{Start: f.frameStart, End: f.frameEnd}, nil
}

func (f *PartitionFramer) SlidingInterval(ctx sql.Context) (sql.WindowInterval, sql.WindowInterval, sql.WindowInterval) {
	panic("implement me")
}

func (f *PartitionFramer) Close() {
	panic("implement me")
}

func NewGroupByFramer() *GroupByFramer {
	return &GroupByFramer{
		frameEnd:       -1,
		frameStart:     -1,
		partitionStart: -1,
		partitionEnd:   -1,
	}
}

type GroupByFramer struct {
	evaluated                    bool
	partitionStart, partitionEnd int

	frameStart, frameEnd int
	partitionSet         bool
}

func (f *GroupByFramer) NewFramer(interval sql.WindowInterval) (sql.WindowFramer, error) {
	return &GroupByFramer{
		evaluated:      false,
		frameEnd:       interval.End,
		frameStart:     interval.Start,
		partitionStart: interval.Start,
		partitionEnd:   interval.End,
		partitionSet:   true,
	}, nil
}

func (f *GroupByFramer) Next(ctx *sql.Context, buffer sql.WindowBuffer) (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, io.EOF
	}
	if !f.evaluated {
		f.evaluated = true
		return f.Interval()
	}
	return sql.WindowInterval{}, io.EOF
}

func (f *GroupByFramer) FirstIdx() int {
	return f.frameStart
}

func (f *GroupByFramer) LastIdx() int {
	return f.frameEnd
}

func (f *GroupByFramer) Interval() (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, ErrPartitionNotSet
	}
	return sql.WindowInterval{Start: f.frameStart, End: f.frameEnd}, nil
}

func (f *GroupByFramer) SlidingInterval(ctx sql.Context) (sql.WindowInterval, sql.WindowInterval, sql.WindowInterval) {
	panic("implement me")
}

// rowFramerBase is a sql.WindowFramer iterator that tracks
// index frames in a sql.WindowBuffer using integer offsets.
// Only a subset of bound conditions will be set for a given
// framer implementation, one start and one end bound.
//
// Ex: startCurrentRow = true; endNFollowing = 1;
//     buffer = [0, 1, 2, 3, 4, 5];
// =>
// pos:    0->0   1->1   2->2   3->3   4->4   5->5
// frame:  {0,2}, {1,3}, {2,4}, {3,5}, {4,6}, {4,5}
// rows:   [0,1], [1,2], [2,3], [3,4], [4,5], [5]
type rowFramerBase struct {
	idx            int
	partitionStart int
	partitionEnd   int
	frameStart     int
	frameEnd       int
	partitionSet   bool

	unboundedFollowing bool
	unboundedPreceding bool
	startCurrentRow    bool
	endCurrentRow      bool

	startOffset int
	endOffset   int

	startNPreceding int
	startNFollowing int
	endNPreceding   int
	endNFollowing   int
}

func (f *rowFramerBase) NewFramer(interval sql.WindowInterval) (sql.WindowFramer, error) {
	var startOffset int
	switch {
	case f.startNPreceding != 0:
		startOffset = -f.startNPreceding
	case f.startNFollowing != 0:
		startOffset = f.startNFollowing
	case f.startCurrentRow:
		startOffset = 0
	}

	var endOffset int
	switch {
	case f.endNPreceding != 0:
		endOffset = -f.endNPreceding
	case f.endNFollowing != 0:
		endOffset = f.endNFollowing
	case f.endCurrentRow:
		endOffset = 0
	}

	return &rowFramerBase{
		idx:            interval.Start,
		partitionStart: interval.Start,
		partitionEnd:   interval.End,
		frameStart:     -1,
		frameEnd:       -1,
		partitionSet:   true,
		// pass through parent state
		unboundedPreceding: f.unboundedPreceding,
		unboundedFollowing: f.unboundedFollowing,
		startCurrentRow:    f.startCurrentRow,
		endCurrentRow:      f.endCurrentRow,
		startNPreceding:    f.startNPreceding,
		startNFollowing:    f.startNFollowing,
		endNPreceding:      f.endNPreceding,
		endNFollowing:      f.endNFollowing,
		// row specific
		startOffset: startOffset,
		endOffset:   endOffset,
	}, nil
}

func (f *rowFramerBase) Next(ctx *sql.Context, buffer sql.WindowBuffer) (sql.WindowInterval, error) {
	if f.idx != 0 && f.idx >= f.partitionEnd || !f.partitionSet {
		return sql.WindowInterval{}, io.EOF
	}

	newStart := f.idx + f.startOffset
	if f.unboundedPreceding || newStart < f.partitionStart {
		newStart = f.partitionStart
	}

	newEnd := f.idx + f.endOffset + 1
	if f.unboundedFollowing || newEnd > f.partitionEnd {
		newEnd = f.partitionEnd
	}

	if newStart > newEnd {
		newStart = newEnd
	}

	f.frameStart = newStart
	f.frameEnd = newEnd

	f.idx++
	return f.Interval()
}

func (f *rowFramerBase) FirstIdx() int {
	return f.frameStart
}

func (f *rowFramerBase) LastIdx() int {
	return f.frameEnd
}

func (f *rowFramerBase) Interval() (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, ErrPartitionNotSet
	}
	return sql.WindowInterval{Start: f.frameStart, End: f.frameEnd}, nil
}

var _ sql.WindowFramer = (*rowFramerBase)(nil)

// rangeFramerBase is a sql.WindowFramer iterator that tracks
// value ranges in a sql.WindowBuffer using bound
// conditions on the order by [expr] column. Only a subset of
// bound conditions will be set for a given framer implementation,
// one start and one end bound.
//
// Ex: startCurrentRow = true; endNFollowing = 2; expr = x;
//  -> startInclusion = (x), endInclusion = (x+2)
//     buffer = [0, 1, 2, 4, 4, 5];
// =>
// pos:    0->0     1->1   2->2   3->4     4->4     5->5
// frame:  {0,3},   {1,3}, {2,3}, {3,5},   {3,5},   {4,5}
// rows:   [0,1,2], [1,2], [2],   [4,4,5], [4,4,5], [5]
type rangeFramerBase struct {
	// order by expression used as range boundary
	expr sql.Expression

	idx                          int
	partitionStart, partitionEnd int
	frameStart, frameEnd         int
	partitionSet                 bool

	unboundedFollowing bool
	unboundedPreceding bool
	startCurrentRow    bool
	endCurrentRow      bool

	startNPreceding sql.Expression
	startNFollowing sql.Expression
	endNPreceding   sql.Expression
	endNFollowing   sql.Expression

	startInclusion sql.Expression
	endInclusion   sql.Expression
}

var _ sql.WindowFramer = (*rangeFramerBase)(nil)

func (f *rangeFramerBase) NewFramer(interval sql.WindowInterval) (sql.WindowFramer, error) {
	var startInclusion sql.Expression
	switch {
	case f.startCurrentRow:
		startInclusion = f.expr
	case f.startNPreceding != nil:
		startInclusion = expression.NewArithmetic(f.expr, f.startNPreceding, ast.MinusStr)
	case f.startNFollowing != nil:
		startInclusion = expression.NewArithmetic(f.expr, f.startNFollowing, ast.PlusStr)
	}

	// TODO: how to validate datetime, interval pair when they aren't type comparable
	//if startInclusion != nil && startInclusion.Type().Promote()  != f.expr.Type().Promote() {
	//	return nil, ErrRangeIntervalTypeMismatch
	//}

	var endInclusion sql.Expression
	switch {
	case f.endCurrentRow:
		endInclusion = f.expr
	case f.endNPreceding != nil:
		endInclusion = expression.NewArithmetic(f.expr, f.endNPreceding, ast.MinusStr)
	case f.endNFollowing != nil:
		endInclusion = expression.NewArithmetic(f.expr, f.endNFollowing, ast.PlusStr)
	}

	// TODO: how to validate datetime, interval pair when they aren't type comparable
	//if endInclusion != nil && endInclusion.Type().Promote() != f.expr.Type().Promote() {
	//	return nil, ErrRangeIntervalTypeMismatch
	//}

	return &rangeFramerBase{
		idx:            interval.Start,
		partitionStart: interval.Start,
		partitionEnd:   interval.End,
		frameStart:     interval.Start,
		frameEnd:       interval.Start,
		partitionSet:   true,
		// pass through parent state
		expr:               f.expr,
		unboundedPreceding: f.unboundedPreceding,
		unboundedFollowing: f.unboundedFollowing,
		startCurrentRow:    f.startCurrentRow,
		endCurrentRow:      f.endCurrentRow,
		startNPreceding:    f.startNPreceding,
		startNFollowing:    f.startNFollowing,
		endNPreceding:      f.endNPreceding,
		endNFollowing:      f.endNFollowing,
		// range specific
		startInclusion: startInclusion,
		endInclusion:   endInclusion,
	}, nil
}

func (f *rangeFramerBase) Next(ctx *sql.Context, buf sql.WindowBuffer) (sql.WindowInterval, error) {
	if f.idx != 0 && f.idx >= f.partitionEnd || !f.partitionSet {
		return sql.WindowInterval{}, io.EOF
	}

	var err error
	newStart := f.frameStart
	switch {
	case newStart < f.partitionStart, f.unboundedPreceding:
		newStart = f.partitionStart
	default:
		newStart, err = findInclusionBoundary(ctx, f.idx, newStart, f.partitionEnd, f.startInclusion, f.expr, buf, greaterThanOrEqual)
		if err != nil {
			return sql.WindowInterval{}, nil
		}
	}

	newEnd := f.frameEnd
	if newStart > newEnd {
		newEnd = newStart
	}
	switch {
	case newEnd > f.partitionEnd, f.unboundedFollowing:
		newEnd = f.partitionEnd
	default:
		newEnd, err = findInclusionBoundary(ctx, f.idx, newEnd, f.partitionEnd, f.endInclusion, f.expr, buf, greaterThan)
		if err != nil {
			return sql.WindowInterval{}, nil
		}
	}

	f.idx++
	f.frameStart = newStart
	f.frameEnd = newEnd
	return f.Interval()
}

type stopCond int

const (
	greaterThan        = 1
	greaterThanOrEqual = 0
)

func findInclusionBoundary(ctx *sql.Context, pos, searchStart, partitionEnd int, inclusion, expr sql.Expression, buf sql.WindowBuffer, stopCond stopCond) (int, error) {
	i := searchStart
	cur, err := inclusion.Eval(ctx, buf[pos])
	if err != nil {
		return 0, err
	}
	for i < partitionEnd {
		res, err := expr.Eval(ctx, buf[i])
		if err != nil {
			return 0, err
		}
		cmp, err := expr.Type().Compare(res, cur)
		if err != nil {
			return 0, err
		}

		if cmp >= int(stopCond) {
			break
		}

		i++
	}
	return i, nil
}

func (f *rangeFramerBase) FirstIdx() int {
	return f.frameStart
}

func (f *rangeFramerBase) LastIdx() int {
	return f.frameEnd
}

func (f *rangeFramerBase) Interval() (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, ErrPartitionNotSet
	}
	return sql.WindowInterval{Start: f.frameStart, End: f.frameEnd}, nil
}
