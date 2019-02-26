package influxql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	regexNeedsQuotes      = `\(([a-zA-Z0-9]+)\)`
	regexContainsFunction = `[a-zA-Z0-9]+\(.*?\)`
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
	limit       *limit
	seriesLimit *limit
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

func (b *builder) AddSelect(column string, alias string) *builder {

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

func (b *builder) AddWhere(field string, symbol string, value interface{}) *builder {

	b.where = append(b.where, Where{
		field:  field,
		symbol: symbol,
		value:  value,
	})

	return b
}

func (b *builder) AddWhereRaw(raw string) *builder {

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

func (b *builder) AddGroupBy(groupBy string) *builder {

	b.groupBy = append(b.groupBy, doubleQuote(groupBy))
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
type fill struct {
	fill   string
	number int
}

func (b *builder) SetFillNull() *builder {

	b.fill = fill{fill: "null"}
	return b
}

func (b *builder) SetFillPrevious() *builder {

	b.fill = fill{fill: "previous"}
	return b
}

func (b *builder) SetFillNumber(number int) *builder {

	b.fill = fill{fill: "number", number: number}
	return b
}

func (b *builder) SetFillNone() *builder {

	b.fill = fill{fill: "none"}
	return b
}

func (b *builder) SetFillLinear() *builder {

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

func (b *builder) SetLimit(l int) *builder {

	x := limit(l)
	b.limit = &x
	return b
}

func (b *builder) SetSeriesLimit(l int) *builder {

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

	field = strings.TrimSuffix(field, `"`)
	field = strings.TrimPrefix(field, `"`)
	return `"` + field + `"`
}
