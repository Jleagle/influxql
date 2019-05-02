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
	if builder.String() != `SELECT func1("col1"), "col2"` {
		t.Error("select field")
	}

	builder.AddSelect("col3", "col3alias")
	if builder.String() != `SELECT func1("col1"), "col2", "col3" as "col3alias"` {
		t.Error("select field alias")
	}

	builder.AddSelect("func1(col1)", "func1_col")
	if builder.String() != `SELECT func1("col1"), "col2", "col3" as "col3alias", func1("col1") as "func1_col"` {
		t.Error("func and alias: " + builder.String())
	}
}

func TestFrom(t *testing.T) {

	builder := NewBuilder()

	builder.SetFrom("x", "y", "z")
	if builder.String() != `FROM "x"."y"."z"` {
		t.Error("from: " + builder.String())
	}

	builder2 := NewBuilder()

	builder2.SetFromSubQuery(builder)
	if builder2.String() != `FROM (FROM "x"."y"."z")` {
		t.Error("from sub: " + builder2.String())
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

func TestOrderBy(t *testing.T) {

	builder := NewBuilder()

	builder.AddOrderBy("col1", true)
	if builder.String() != `ORDER BY "col1" ASC` {
		t.Error("order by asc")
	}

	builder.AddOrderBy("col1", false)
	if builder.String() != `ORDER BY "col1" ASC, "col1" DESC` {
		t.Error("order by desc")
	}

	builder.AddOrderBy("func1(col1)", true)
	if builder.String() != `ORDER BY "col1" ASC, "col1" DESC, func1("col1") ASC` {
		t.Error("order by func")
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

	builder.AddWhere("time", ">", "NOW()-7d")
	if builder.String() != `WHERE "col1" = '1' AND f("col2") = '2' AND col3 = '3' AND time > NOW()-7d` {
		t.Error("where time: " + builder.String())
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
