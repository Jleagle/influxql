package influxql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	regexNeedsQuotes      = `\(([a-zA-Z0-9_]+)\)`
	regexContainsFunction = `[a-zA-Z0-9_]+\(.*?\)`
)

func NewBuilder() *Builder {
	return &Builder{}
}

// Builder
type Builder struct {
	fields      fields
	from        from
	groupBys    groupBys
	orderBys    orderBys
	conditions  conditions
	fill        fill
	limit       *limit
	seriesLimit *limit
}

func (b Builder) String() string {

	var ret []string

	if len(b.fields) > 0 {
		ret = append(ret, b.fields.string())
	}

	if b.from != (from{}) {
		ret = append(ret, b.from.string())
	}

	if len(b.conditions) > 0 {
		ret = append(ret, b.conditions.string())
	}

	if len(b.groupBys) > 0 {
		ret = append(ret, b.groupBys.string())
	}

	if len(b.orderBys) > 0 {
		ret = append(ret, b.orderBys.string())
	}

	if b.fill != "" {
		ret = append(ret, b.fill.string())
	}

	if b.limit != nil {
		ret = append(ret, b.limit.string(false))
	}

	if b.seriesLimit != nil {
		ret = append(ret, b.seriesLimit.string(true))
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

	return "SELECT " + strings.Join(ret, ", ")
}

type field struct {
	column string
	alias  string
}

func (b *Builder) AddSelect(column string, alias string) *Builder {

	b.fields = append(b.fields, field{
		column: column,
		alias:  alias,
	})

	return b
}

func (f field) string() string {

	ret := doubleQuote(f.column)

	if f.alias != "" {
		ret += " as " + doubleQuote(f.alias)
	}

	return ret
}

// From
type from struct {
	database    string
	retention   string
	measurement string
	builder     *Builder
}

func (b *Builder) SetFrom(db string, retention string, measurement string) *Builder {

	b.from = from{
		database:    db,
		retention:   retention,
		measurement: measurement,
	}

	return b
}

func (b *Builder) SetFromSubQuery(builder *Builder) *Builder {

	b.from = from{
		builder: builder,
	}

	return b
}

func (f from) string() (str string) {

	if f.builder != nil {
		return "FROM (" + f.builder.String() + ")"
	}

	var ret []string
	if f.database != "" {
		ret = append(ret, doubleQuote(f.database))
	}
	if f.retention != "" {
		ret = append(ret, doubleQuote(f.retention))
	}
	if f.measurement != "" {
		ret = append(ret, doubleQuote(f.measurement))
	}

	return "FROM " + strings.Join(ret, ".")
}

// Where
type conditions []Where

func (f conditions) string() string {

	var ret []string

	for _, field := range f {
		ret = append(ret, field.string())
	}

	return "WHERE " + strings.Join(ret, " AND ")
}

type Where struct {
	field  string
	symbol string
	value  interface{}
	raw    string
	or     conditions
}

func (b *Builder) AddWhere(field string, symbol string, value interface{}) *Builder {

	b.conditions = append(b.conditions, Where{
		field:  field,
		symbol: symbol,
		value:  value,
	})

	return b
}

func (b *Builder) AddWhereRaw(raw string) *Builder {

	b.conditions = append(b.conditions, Where{raw: raw})
	return b
}

func (w Where) string() string {

	if w.raw != "" {
		return w.raw
	}

	value := fmt.Sprint(w.value)

	if !regexp.MustCompile(regexContainsFunction).MatchString(value) {
		value = "'" + fmt.Sprint(w.value) + "'"
	}

	return doubleQuote(w.field) + " " + string(w.symbol) + " " + value
}

// Group By
type groupBys []string

func (b *Builder) AddGroupBy(groupBy string) *Builder {

	b.groupBys = append(b.groupBys, doubleQuote(groupBy))
	return b
}

func (b *Builder) AddGroupByTime(time string) *Builder {

	b.AddGroupBy("time(" + time + ")")
	return b
}

func (g groupBys) string() string {

	return "GROUP BY " + strings.Join(g, ", ")
}

// Order By
type orderBys []order

type order struct {
	field     string
	ascending bool // true = ascending
}

func (o order) string() string {

	ret := doubleQuote(o.field)

	if o.ascending {
		return ret + " ASC"
	}

	return ret + " DESC"
}

func (b *Builder) AddOrderBy(field string, ascending bool) *Builder {

	b.orderBys = append(b.orderBys, order{
		field:     field,
		ascending: ascending,
	})
	return b
}

func (o orderBys) string() string {

	var ret []string

	for _, order := range o {
		ret = append(ret, order.string())
	}

	return "ORDER BY " + strings.Join(ret, ", ")
}

// Fill
type fill string

func (b *Builder) SetFillNull() *Builder {

	b.fill = "null"
	return b
}

func (b *Builder) SetFillPrevious() *Builder {

	b.fill = "previous"
	return b
}

func (b *Builder) SetFillNumber(number int) *Builder {

	b.fill = fill(strconv.Itoa(number))
	return b
}

func (b *Builder) SetFillNone() *Builder {

	b.fill = "none"
	return b
}

func (b *Builder) SetFillLinear() *Builder {

	b.fill = "linear"
	return b
}

func (f fill) string() string {

	return "FILL(" + string(f) + ")"
}

// Limit
type limit int

func (b *Builder) SetLimit(l int) *Builder {

	x := limit(l)
	b.limit = &x
	return b
}

func (b *Builder) SetSeriesLimit(l int) *Builder {

	x := limit(l)
	b.seriesLimit = &x
	return b
}

func (l limit) string(series bool) string {

	ret := "LIMIT " + strconv.Itoa(int(l))

	if series {
		return "S" + ret
	}
	return ret
}

//
func doubleQuote(field string) string {

	if strings.HasPrefix(field, "time") {
		return strings.Replace(field, `"`, "", -1)
	}

	re := regexp.MustCompile(regexNeedsQuotes)
	if re.MatchString(field) {
		return re.ReplaceAllString(field, `("$1")`)
	}

	if regexp.MustCompile(regexContainsFunction).MatchString(field) {
		return field
	}

	field = strings.TrimSuffix(field, `"`)
	field = strings.TrimPrefix(field, `"`)
	return `"` + field + `"`
}
