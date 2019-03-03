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
	groupBy     groupBy
	where       conditions
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

	if len(b.where) > 0 {
		ret = append(ret, b.where.string())
	}

	if len(b.groupBy) > 0 {
		ret = append(ret, b.groupBy.string())
	}

	if b.fill != (fill{}) {
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
}

func (b *Builder) SetFrom(db string, retention string, measurement string) *Builder {

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
}

func (b *Builder) AddWhere(field string, symbol string, value interface{}) *Builder {

	b.where = append(b.where, Where{
		field:  field,
		symbol: symbol,
		value:  value,
	})

	return b
}

func (b *Builder) AddWhereRaw(raw string) *Builder {

	b.where = append(b.where, Where{raw: raw})
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
type groupBy []string

func (b *Builder) AddGroupBy(groupBy string) *Builder {

	b.groupBy = append(b.groupBy, doubleQuote(groupBy))
	return b
}

func (b *Builder) AddGroupByTime(time string) *Builder {

	b.AddGroupBy("time(" + time + ")")
	return b
}

func (g groupBy) string() string {

	return "GROUP BY " + strings.Join(g, ", ")
}

// Fill
type fill struct {
	fill   string
	number int
}

func (b *Builder) SetFillNull() *Builder {

	b.fill = fill{fill: "null"}
	return b
}

func (b *Builder) SetFillPrevious() *Builder {

	b.fill = fill{fill: "previous"}
	return b
}

func (b *Builder) SetFillNumber(number int) *Builder {

	b.fill = fill{fill: "number", number: number}
	return b
}

func (b *Builder) SetFillNone() *Builder {

	b.fill = fill{fill: "none"}
	return b
}

func (b *Builder) SetFillLinear() *Builder {

	b.fill = fill{fill: "linear"}
	return b
}

func (f fill) string() string {

	if f.number == 0 {
		return "FILL(" + f.fill + ")"
	}
	return "FILL(" + f.fill + ", " + strconv.Itoa(f.number) + ")"
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
