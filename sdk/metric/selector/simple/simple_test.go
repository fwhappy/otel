// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simple_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fwhappy/otel/metric"
	"github.com/fwhappy/otel/metric/number"
	export "github.com/fwhappy/otel/sdk/export/metric"
	"github.com/fwhappy/otel/sdk/metric/aggregator/array"
	"github.com/fwhappy/otel/sdk/metric/aggregator/ddsketch"
	"github.com/fwhappy/otel/sdk/metric/aggregator/histogram"
	"github.com/fwhappy/otel/sdk/metric/aggregator/lastvalue"
	"github.com/fwhappy/otel/sdk/metric/aggregator/minmaxsumcount"
	"github.com/fwhappy/otel/sdk/metric/aggregator/sum"
	"github.com/fwhappy/otel/sdk/metric/selector/simple"
)

var (
	testCounterDesc           = metric.NewDescriptor("counter", metric.CounterInstrumentKind, number.Int64Kind)
	testUpDownCounterDesc     = metric.NewDescriptor("updowncounter", metric.UpDownCounterInstrumentKind, number.Int64Kind)
	testSumObserverDesc       = metric.NewDescriptor("sumobserver", metric.SumObserverInstrumentKind, number.Int64Kind)
	testUpDownSumObserverDesc = metric.NewDescriptor("updownsumobserver", metric.UpDownSumObserverInstrumentKind, number.Int64Kind)
	testValueRecorderDesc     = metric.NewDescriptor("valuerecorder", metric.ValueRecorderInstrumentKind, number.Int64Kind)
	testValueObserverDesc     = metric.NewDescriptor("valueobserver", metric.ValueObserverInstrumentKind, number.Int64Kind)
)

func oneAgg(sel export.AggregatorSelector, desc *metric.Descriptor) export.Aggregator {
	var agg export.Aggregator
	sel.AggregatorFor(desc, &agg)
	return agg
}

func testFixedSelectors(t *testing.T, sel export.AggregatorSelector) {
	require.IsType(t, (*lastvalue.Aggregator)(nil), oneAgg(sel, &testValueObserverDesc))
	require.IsType(t, (*sum.Aggregator)(nil), oneAgg(sel, &testCounterDesc))
	require.IsType(t, (*sum.Aggregator)(nil), oneAgg(sel, &testUpDownCounterDesc))
	require.IsType(t, (*sum.Aggregator)(nil), oneAgg(sel, &testSumObserverDesc))
	require.IsType(t, (*sum.Aggregator)(nil), oneAgg(sel, &testUpDownSumObserverDesc))
}

func TestInexpensiveDistribution(t *testing.T) {
	inex := simple.NewWithInexpensiveDistribution()
	require.IsType(t, (*minmaxsumcount.Aggregator)(nil), oneAgg(inex, &testValueRecorderDesc))
	testFixedSelectors(t, inex)
}

func TestSketchDistribution(t *testing.T) {
	sk := simple.NewWithSketchDistribution(ddsketch.NewDefaultConfig())
	require.IsType(t, (*ddsketch.Aggregator)(nil), oneAgg(sk, &testValueRecorderDesc))
	testFixedSelectors(t, sk)
}

func TestExactDistribution(t *testing.T) {
	ex := simple.NewWithExactDistribution()
	require.IsType(t, (*array.Aggregator)(nil), oneAgg(ex, &testValueRecorderDesc))
	testFixedSelectors(t, ex)
}

func TestHistogramDistribution(t *testing.T) {
	hist := simple.NewWithHistogramDistribution(nil)
	require.IsType(t, (*histogram.Aggregator)(nil), oneAgg(hist, &testValueRecorderDesc))
	testFixedSelectors(t, hist)
}
