package dea

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type Funnel struct {
	core utils.QueryGenerator

	inOrder            bool
	minSteps, maxSteps int
}

func NewFunnel(inOrder bool, minSteps, maxSteps int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &Funnel{
			core:     core,
			inOrder:  inOrder,
			minSteps: minSteps,
			maxSteps: maxSteps,
		}
	}
}

func (f *Funnel) Fill(q query.Query) query.Query {
	fc, ok := f.core.(FunnelFiller)
	if !ok {
		common.PanicUnimplementedQuery(f.core)
	}
	fc.Funnel(q, f.inOrder, f.minSteps, f.maxSteps)

	return q
}
