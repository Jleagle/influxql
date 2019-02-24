package influxql

import (
	"fmt"
	"strings"
)

func NewBuilder() *builder {
	return &builder{}
}

// Builder
type builder struct {
	fields      fields
	from        from
	groupBy     groupBy
	where       conditions
	fill        fill
	limit       *int
	seriesLimit *int
}

func (b builder) String() string {

	var ret []string

	if len(b.fields) > 0 {
		ret = append(ret, b.fields.string())
	}

	if b.from != (from{}) {
		ret = append(ret, b.from.string())
	}

	if len(b.where) > 0 {
		ret = append(ret, b.where.string())
	}

	if len(b.groupBy) > 0 {
		ret = append(ret, b.groupBy.string())
	}

	if b.fill != "" {
		ret = append(ret, b.fill.string())
	}

	return strings.Join(ret, " ")
}

// Select
type fields []field

func (f fields) string() string {

	var ret []string

	for _, field := range f {
		ret = append(ret, field.string())
	}

	return strings.Join(ret, ", ")
}

type field struct {
	column string
	alias  string
}

func (b *builder) AddSelect(column string, alias string) *builder {

	b.fields = append(b.fields, field{
		column: column,
		alias:  alias,
	})

	return b
}

func (f field) string() string {

	ret := f.column

	if strings.ContainsAny(ret, "()") {

		if !strings.ContainsAny(ret, `"`) {
			ret = strings.Replace(ret, ")", `")`, 1)
			ret = strings.Replace(ret, "(", `("`, 1)
		}

	} else {

		ret = `"` + ret + `"`
	}

	if f.alias != "" {
		ret += ` as "` + f.alias + `"`
	}

	return "SELECT " + ret

}

// From
type from struct {
	database    string
	retention   string
	measurement string
}

func (b *builder) SetFrom(db string, retention string, measurement string) *builder {

	b.from = from{
		database:    db,
		retention:   retention,
		measurement: measurement,
	}

	return b
}

func (f from) string() (str string) {

	var ret []string
	if f.database != "" {
		ret = append(ret, `"`+f.database+`"`)
	}
	if f.retention != "" {
		ret = append(ret, `"`+f.retention+`"`)
	}
	if f.measurement != "" {
		ret = append(ret, `"`+f.measurement+`"`)
	}

	return "FROM " + strings.Join(ret, ".")
}

// Where
type Where struct {
	field  string
	symbol string
	value  interface{}
}

func (b *builder) AddWhere(field string, symbol string, value interface{}) *builder {

	b.where = append(b.where, Where{
		field:  field,
		symbol: symbol,
		value:  value,
	})

	return b
}

type conditions []Where

func (f conditions) string() string {

	var ret []string

	for _, field := range f {
		ret = append(ret, field.string())
	}

	return "WHERE " + strings.Join(ret, " AND ")
}

func (w Where) string() string {

	field := w.field
	if !strings.ContainsAny(field, "()-+*/") {
		field = `"` + field + `"`
	}

	value := fmt.Sprint(w.value)
	if !strings.ContainsAny(value, "()-+*/") {
		value = "'" + fmt.Sprint(w.value) + "'"
	}

	return field + " " + string(w.symbol) + " " + value
}

// Group By
type groupBy []string

func (b *builder) AddGroupBy(groupBy string) *builder {

	if strings.ContainsAny(groupBy, "()") {

		if !strings.ContainsAny(groupBy, `"`) && !strings.ContainsAny(groupBy, `time(`) {
			groupBy = strings.Replace(groupBy, ")", `")`, 1)
			groupBy = strings.Replace(groupBy, "(", `("`, 1)
		}

	} else {

		groupBy = `"` + groupBy + `"`
	}

	b.groupBy = append(b.groupBy, groupBy)

	return b
}

func (b *builder) AddGroupByTime(time string) *builder {

	b.AddGroupBy("time(" + time + ")")
	return b
}

func (g groupBy) string() string {

	return "GROUP BY " + strings.Join(g, ", ")
}

// Fill
type fill string

const (
	FillNull     fill = "null"
	FillPrevious      = "previous"
	FillNumbers       = "number"
	FillNone          = "none"
	FillLinear        = "linear"
)

func (b *builder) SetFill(fill fill, number ...int) *builder {

	b.fill = fill

	return b
}

func (f fill) string() string {
	return "FILL(" + string(f) + ")"
}
