package influxql

import (
	"testing"
)

func TestSelect(t *testing.T) {

	builder := NewBuilder()

	builder.AddSelect("func1(col1)", "")
	if builder.String() != `SELECT func1("col1")` {
		t.Error("select quoting")
	}

	builder.AddSelect("col2", "")
	if builder.String() != `SELECT func1("col1"), SELECT "col2"` {
		t.Error("select field")
	}

	builder.AddSelect("col3", "col3alias")
	if builder.String() != `SELECT func1("col1"), SELECT "col2", SELECT "col3" as "col3alias"` {
		t.Error("select field alias")
	}
}

func TestFrom(t *testing.T) {

	builder := NewBuilder()

	builder.SetFrom("x", "y", "z")
	if builder.String() != `FROM "x"."y"."z"` {
		t.Error("from")
	}
}

func TestGroupBy(t *testing.T) {

	builder := NewBuilder()

	builder.AddGroupBy("col1")
	if builder.String() != `GROUP BY "col1"` {
		t.Error("group by")
	}

	builder.AddGroupByTime("10m")
	if builder.String() != `GROUP BY "col1", time(10m)` {
		t.Error("group by time")
	}
}

func TestWhere(t *testing.T) {

	builder := NewBuilder()

	builder.AddWhere("col1", "=", 1)
	if builder.String() != `WHERE "col1" = '1'` {
		t.Error("where")
	}

	builder.AddWhere("f(col2)", "=", 2)
	if builder.String() != `WHERE "col1" = '1' AND f("col2") = '2'` {
		t.Error("where func")
	}

	builder.AddWhereRaw("col3 = '3'")
	if builder.String() != `WHERE "col1" = '1' AND f("col2") = '2' AND col3 = '3'` {
		t.Error("where raw")
	}
}

func TestFill(t *testing.T) {

	builder := NewBuilder()

	builder.SetFillNone()
	if builder.String() != `FILL(none)` {
		t.Error("fill none")
	}

	builder.SetFillNumber(2)
	if builder.String() != `FILL(number, 2)` {
		t.Error("fill number")
	}
}

func TestLmits(t *testing.T) {

	builder := NewBuilder()

	builder.SetLimit(2)
	if builder.String() != `LIMIT 2` {
		t.Error("limit")
	}

	builder.SetSeriesLimit(2)
	if builder.String() != `LIMIT 2 SLIMIT 2` {
		t.Error("series limit")
	}
}
