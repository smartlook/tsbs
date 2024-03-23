package clickhouse

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/dea"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for ClickHouse.
type BaseGenerator struct {
	UseTags            bool
	PropertyAccessMode string
}

// GenerateEmptyQuery returns an empty query.ClickHouse.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewClickHouse()
}

// fill Query fills the query struct with data
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, table, sql string) {
	q := qi.(*query.ClickHouse)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Table = []byte(table)
	q.SqlQuery = []byte(sql)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// NewDEA creates a new devops use case query generator.
func (g *BaseGenerator) NewDEA(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := dea.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &DEA{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// ClickHouse understands and can compare time presented as strings of this format
const clickhouseTimeStringFormat = "2006-01-02 15:04:05"
