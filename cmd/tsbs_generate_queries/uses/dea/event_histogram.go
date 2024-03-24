package dea

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type EventHistogram struct {
	core utils.QueryGenerator
}

func NewEventHistogram() utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &EventHistogram{
			core: core,
		}
	}
}

func (i *EventHistogram) Fill(q query.Query) query.Query {
	fc, ok := i.core.(EventHistogramFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.EventHistogram(q)

	return q
}
