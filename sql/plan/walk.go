package plan

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Visitor visits nodes in the plan.
type Visitor interface {
	// Visit method is invoked for each node encountered by Walk.
	// If the result Visitor is not nil, Walk visits each of the children
	// of the node with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	Visit(node sql.Node) Visitor
}

// Walk traverses the plan tree in depth-first order. It starts by calling
// v.Visit(node); node must not be nil. If the visitor returned by
// v.Visit(node) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the node, followed by a call of v.Visit(nil)
// to the returned visitor.
func Walk(v Visitor, node sql.Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	for _, child := range node.Children() {
		Walk(v, child)
	}

	v.Visit(nil)
}

type inspector func(sql.Node) bool

func (f inspector) Visit(node sql.Node) Visitor {
	if f(node) {
		return f
	}
	return nil
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(node); node must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of node, followed by a call of
// f(nil).
func Inspect(node sql.Node, f func(sql.Node) bool) {
	Walk(inspector(f), node)
}

// WalkExpressions traverses the plan and calls sql.Walk on any expression it finds.
func WalkExpressions(v sql.Visitor, node sql.Node) {
	Inspect(node, func(node sql.Node) bool {
		if n, ok := node.(sql.Expressioner); ok {
			for _, e := range n.Expressions() {
				sql.Walk(v, e)
			}
		}
		return true
	})
}

// WalkExpressionsWithNode traverses the plan and calls sql.WalkWithNode on any expression it finds.
func WalkExpressionsWithNode(v sql.NodeVisitor, n sql.Node) {
	Inspect(n, func(n sql.Node) bool {
		if expressioner, ok := n.(sql.Expressioner); ok {
			for _, e := range expressioner.Expressions() {
				sql.WalkWithNode(v, n, e)
			}
		}
		return true
	})
}

// InspectExpressions traverses the plan and calls sql.Inspect on any
// expression it finds.
func InspectExpressions(node sql.Node, f func(sql.Expression) bool) {
	WalkExpressions(exprInspector(f), node)
}

type exprInspector func(sql.Expression) bool

func (f exprInspector) Visit(e sql.Expression) sql.Visitor {
	if f(e) {
		return f
	}
	return nil
}

// InspectExpressionsWithNode traverses the plan and calls sql.Inspect on any expression it finds.
func InspectExpressionsWithNode(node sql.Node, f func(sql.Node, sql.Expression) bool) {
	WalkExpressionsWithNode(exprWithNodeInspector(f), node)
}

type exprWithNodeInspector func(sql.Node, sql.Expression) bool

func (f exprWithNodeInspector) Visit(n sql.Node, e sql.Expression) sql.NodeVisitor {
	if f(n, e) {
		return f
	}
	return nil
}

