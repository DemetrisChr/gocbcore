package gocbcore

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type parsedTelemetryCounterEntry struct {
	name      string
	count     uint64
	timestamp uint64
	tags      map[string]string
}

func (suite *StandardTestSuite) parseTelemetryEntry(entry string) parsedTelemetryCounterEntry {
	name, rest, found := strings.Cut(entry, "{")
	suite.Assert().True(found)

	rawTags, rest, found := strings.Cut(rest, "}")
	suite.Assert().True(found)

	nums := strings.Split(strings.TrimSpace(rest), " ")

	count, err := strconv.ParseUint(nums[0], 10, 64)
	suite.Assert().NoError(err)

	timestamp, err := strconv.ParseUint(nums[1], 10, 64)
	suite.Assert().NoError(err)

	res := parsedTelemetryCounterEntry{
		name:      name,
		count:     count,
		timestamp: timestamp,
	}

	for _, rawTag := range strings.Split(rawTags, ",") {
		pair := strings.Split(rawTag, "=")
		suite.Assert().Len(pair, 2)
		res.tags[pair[0]] = pair[1]
	}

	return res
}

func (suite *StandardTestSuite) TestTelemetryCounterSerialization() {
	counters := newTelemetryCounters()

	counters.recordOp(TelemetryOperationAttributes{
		outcome: ErrRequestCanceled,
		node:    "node1",
		service: N1qlService,
	})
	counters.recordOp(TelemetryOperationAttributes{
		outcome: ErrRequestCanceled,
		node:    "node1",
		service: N1qlService,
	})
	counters.recordOp(TelemetryOperationAttributes{
		outcome: ErrRequestCanceled,
		node:    "node2",
		service: N1qlService,
	})
	counters.recordOp(TelemetryOperationAttributes{
		outcome: ErrRequestCanceled,
		node:    "node1",
		service: MemdService,
		bucket:  "default",
	})
	counters.recordOp(TelemetryOperationAttributes{
		outcome: ErrAmbiguousTimeout,
		node:    "node1",
		service: MemdService,
		bucket:  "default",
	})
	counters.recordOp(TelemetryOperationAttributes{
		outcome: nil,
		node:    "node2",
		service: MemdService,
		bucket:  "default",
	})

	timestamp := time.Now().Unix()
	agent := "sdk/1.2.3"

	serialized := counters.serialize(timestamp, agent)

	expected := []string{
		fmt.Sprintf("sdk_query_r_canceled{agent=\"sdk/1.2.3\", node=\"node1\"} 2 %d", timestamp),
		fmt.Sprintf("sdk_query_r_canceled{agent=\"sdk/1.2.3\", node=\"node2\"} 1 %d", timestamp),
		fmt.Sprintf("sdk_kv_r_canceled{agent=\"sdk/1.2.3\", node=\"node1\", bucket=\"default\"} 1 %d", timestamp),
		fmt.Sprintf("sdk_kv_r_atimedout{agent=\"sdk/1.2.3\", node=\"node1\", bucket=\"default\"} 1 %d", timestamp),
		fmt.Sprintf("sdk_query_r_total{agent=\"sdk/1.2.3\", node=\"node1\"} 2 %d", timestamp),
		fmt.Sprintf("sdk_query_r_total{agent=\"sdk/1.2.3\", node=\"node2\"} 1 %d", timestamp),
		fmt.Sprintf("sdk_kv_r_total{agent=\"sdk/1.2.3\", node=\"node1\", bucket=\"default\"} 2 %d", timestamp),
		fmt.Sprintf("sdk_kv_r_total{agent=\"sdk/1.2.3\", node=\"node2\", bucket=\"default\"} 1 %d", timestamp),
	}

	suite.Assert().ElementsMatch(expected, serialized)
}
