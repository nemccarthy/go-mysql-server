package plan

import (
	"io"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// ErrInsertIntoNotSupported is thrown when a table doesn't support inserts
var ErrInsertIntoNotSupported = errors.NewKind("table doesn't support INSERT INTO")
var ErrReplaceIntoNotSupported = errors.NewKind("table doesn't support REPLACE INTO")
var ErrInsertIntoMismatchValueCount = errors.NewKind("number of values does not match number of columns provided")
var ErrInsertIntoUnsupportedValues = errors.NewKind("%T is unsupported for inserts")
var ErrInsertIntoDuplicateColumn = errors.NewKind("duplicate column name %v")
var ErrInsertIntoNonexistentColumn = errors.NewKind("invalid column name %v")
var ErrInsertIntoNonNullableDefaultNullColumn = errors.NewKind("column name '%v' is non-nullable but attempted to set default value of null")
var ErrInsertIntoNonNullableProvidedNull = errors.NewKind("column name '%v' is non-nullable but attempted to set a value of null")
var ErrInsertIntoIncompatibleTypes = errors.NewKind("cannot convert type %s to %s")

// InsertInto is a node describing the insertion into some table.
type InsertInto struct {
	BinaryNode
	ColumnNames []string
	IsReplace   bool
	Columns     []sql.Expression
}

// NewInsertInto creates an InsertInto node.
func NewInsertInto(dst, src sql.Node, isReplace bool, cols []string) *InsertInto {
	return &InsertInto{
		BinaryNode:  BinaryNode{Left: dst, Right: src},
		ColumnNames: cols,
		IsReplace:   isReplace,
	}
}

// Schema implements the sql.Node interface.
// Insert nodes return rows that are inserted. Replaces return a concatenation of the deleted row and the inserted row.
// If no row was deleted, the value of those columns is nil.
func (p *InsertInto) Schema() sql.Schema {
	if p.IsReplace {
		return append(p.Left.Schema(), p.Left.Schema()...)
	}
	return p.Left.Schema()
}

type insertIter struct {
	schema     sql.Schema
	inserter   sql.RowInserter
	replacer   sql.RowReplacer
	rowSource  sql.RowIter
	ctx        *sql.Context
	projection []sql.Expression
}

func GetInsertable(node sql.Node) (sql.InsertableTable, error) {
	switch node := node.(type) {
	case *Exchange:
		return GetInsertable(node.Child)
	case sql.InsertableTable:
		return node, nil
	case *ResolvedTable:
		return getInsertableTable(node.Table)
	case *prependNode:
		return GetInsertable(node.Child)
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func getInsertableTable(t sql.Table) (sql.InsertableTable, error) {
	switch t := t.(type) {
	case sql.InsertableTable:
		return t, nil
	case sql.TableWrapper:
		return getInsertableTable(t.Underlying())
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func newInsertIter(ctx *sql.Context, table sql.Node, values sql.Node, isReplace bool, columns []sql.Expression, row sql.Row) (*insertIter, error) {
	dstSchema := table.Schema()

	insertable, err := GetInsertable(table)
	if err != nil {
		return nil, err
	}

	var inserter sql.RowInserter
	var replacer sql.RowReplacer
	if isReplace {
		replacer = insertable.(sql.ReplaceableTable).Replacer(ctx)
	} else {
		inserter = insertable.(sql.InsertableTable).Inserter(ctx)
	}

	rowIter, err := values.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &insertIter{
		schema:     dstSchema,
		inserter:   inserter,
		replacer:   replacer,
		rowSource:  rowIter,
		projection: columns,
		ctx:        ctx,
	}, nil
}

func (i insertIter) Next() (sql.Row, error) {
	row, err := i.rowSource.Next()
	if err == io.EOF {
		return nil, err
	}

	if err != nil {
		_ = i.rowSource.Close()
		return nil, err
	}

	row, err = ProjectRow(i.ctx, i.projection, row)
	if err != nil {
		return nil, err
	}

	err = validateNullability(i.schema, row)
	if err != nil {
		_ = i.rowSource.Close()
		return nil, err
	}

	// Do any necessary type conversions to the target schema
	for i, col := range i.schema {
		if row[i] != nil {
			row[i], err = col.Type.Convert(row[i])
			if err != nil {
				return nil, err
			}
		}
	}

	if i.replacer != nil {
		toReturn := row.Append(row)
		if err = i.replacer.Delete(i.ctx, row); err != nil {
			if !sql.ErrDeleteRowNotFound.Is(err) {
				_ = i.rowSource.Close()
				return nil, err
			}
			// if the row was not found during deletion, write nils into the toReturn row
			for i := range row {
				toReturn[i] = nil
			}
		}

		if err = i.replacer.Insert(i.ctx, row); err != nil {
			_ = i.rowSource.Close()
			return nil, err
		}
		return toReturn, nil
	} else {
		if err := i.inserter.Insert(i.ctx, row); err != nil {
			_ = i.rowSource.Close()
			return nil, err
		}
		return row, nil
	}
}

func (i insertIter) Close() error {
	if i.inserter != nil {
		if err := i.inserter.Close(i.ctx); err != nil {
			return err
		}
	}
	if i.replacer != nil {
		if err := i.replacer.Close(i.ctx); err != nil {
			return err
		}
	}
	if i.rowSource != nil {
		if err := i.rowSource.Close(); err != nil {
			return err
		}
	}

	return nil
}

// RowIter implements the Node interface.
func (p *InsertInto) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return newInsertIter(ctx, p.Left, p.Right, p.IsReplace, p.Columns, row)
}

// WithChildren implements the Node interface.
func (p *InsertInto) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}

	np := *p
	np.Left, np.Right = children[0], children[1]
	return &np, nil
}

// WithColumsn returns a copy of this node with the given column expressions applied.
// TODO: replace with sql.Expressioner?
func (p *InsertInto) WithColumns(columns []sql.Expression) (sql.Node, error) {
	np := *p
	np.Columns = columns
	return &np, nil
}

func (p InsertInto) String() string {
	pr := sql.NewTreePrinter()
	if p.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(p.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(p.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(p.Left.String(), p.Right.String())
	return pr.String()
}

func (p InsertInto) DebugString() string {
	pr := sql.NewTreePrinter()
	if p.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(p.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(p.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(sql.DebugString(p.Left), sql.DebugString(p.Right))
	return pr.String()
}

func validateNullability(dstSchema sql.Schema, row sql.Row) error {
	for i, col := range dstSchema {
		if !col.Nullable && row[i] == nil {
			return ErrInsertIntoNonNullableProvidedNull.New(col.Name)
		}
	}
	return nil
}
