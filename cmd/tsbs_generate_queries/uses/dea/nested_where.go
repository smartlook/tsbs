package dea

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// NestedWhere contains info for filling in avg load queries.
type NestedWhere struct {
	core utils.QueryGenerator
}

// NewNestedWhere creates a new avg load query filler.
func NewNestedWhere() utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &NestedWhere{
			core: core,
		}
	}
}

// Fill fills in the query.Query with query details.
func (i *NestedWhere) Fill(q query.Query) query.Query {
	fc, ok := i.core.(NestedWhereFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.NestedWhere(q)
	return q
}
