package stats

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type DefaultIMetrics struct{}

func (d DefaultIMetrics) Get() []string {
	return []string{"My Phase 1 Conflicts",
		"My Phase 2 Conflicts",
		"Ballot Choosing Attempts",
		"Noop Proposed",
		"Client Value Proposed",
		"Previous Value Proposed"}
}

type InstanceID struct {
	Log int32
	Seq int32
}

//stats to collect
// noop proposed
// value proposed
// how many client values proposed
// conflicts us had first phase
// conflicts us had second phase
// how many attempts to choose in final
// I began proposing at?
//

type TimeTuple struct {
	Open    time.Time
	Commit  time.Time
	Execute time.Time
}

type CommitExecutionComparator struct {
	cmdsTimes map[InstanceID]TimeTuple
}

func CommitExecutionComparatorNew() *CommitExecutionComparator {
	return &CommitExecutionComparator{
		cmdsTimes: make(map[InstanceID]TimeTuple),
	}
}

func (c *CommitExecutionComparator) recordOpen(id InstanceID, time time.Time) {
	timeTup, _ := c.cmdsTimes[id]
	if !timeTup.Open.IsZero() {
		panic("cannot overwrite open time")
	}
	c.cmdsTimes[id] = TimeTuple{Open: time}
}

func (c *CommitExecutionComparator) recordCommit(id InstanceID, time time.Time) {
	timeTup, _ := c.cmdsTimes[id]
	if !timeTup.Commit.IsZero() {
		panic("cannot overwrite commit time")
	}
	c.cmdsTimes[id] = TimeTuple{
		Open:   timeTup.Open,
		Commit: time,
	}
}

func (c *CommitExecutionComparator) recordExecution(id InstanceID, time time.Time) {
	timeTup, exists := c.cmdsTimes[id]
	if !exists {
		panic("somehow recording execution when not first recorded commiting")
	}
	if !timeTup.Execute.IsZero() {
		panic("cannot overwrite execution time")
	}
	c.cmdsTimes[id] = TimeTuple{
		Open:    timeTup.Open,
		Commit:  timeTup.Commit,
		Execute: time,
	}
	//	c.outputInstanceTimes(id)
}

func (c *CommitExecutionComparator) outputInstanceTimes(id InstanceID) string {
	timeTup, exists := c.cmdsTimes[id]
	if !exists {
		panic("cannot output a command with no record of commit or execution")
	}
	cmtDiff := timeTup.Commit.Sub(timeTup.Open)
	execDiff := timeTup.Execute.Sub(timeTup.Commit)
	return fmt.Sprintf("%d, %d, %d, %d, %d", timeTup.Open.UnixNano(), timeTup.Commit.UnixNano(), timeTup.Execute.UnixNano(), cmtDiff.Microseconds(), execDiff.Microseconds())
}

func (c *CommitExecutionComparator) getOutputFields() string {
	return "Open Time, Commit Time, Execute Time, Open-Commit Latency, Commit-Execute Latency"
}

type InstanceStats struct {
	outputFile *os.File
	*CommitExecutionComparator
	register    map[InstanceID]map[string]int
	orderedKeys []string
}

func InstanceStatsNew(outputLoc string, registerIDs []string) *InstanceStats {
	file, _ := os.Create(outputLoc)

	instanceStats := &InstanceStats{
		outputFile:                file,
		CommitExecutionComparator: CommitExecutionComparatorNew(),
		orderedKeys:               registerIDs,
	}

	str := strings.Builder{}
	str.WriteString("Log ID, Log Seq No")
	for i := 0; i < len(registerIDs); i++ {
		str.WriteString(fmt.Sprintf(", %s", registerIDs[i]))
	}
	str.WriteString(", ")
	str.WriteString(instanceStats.CommitExecutionComparator.getOutputFields())
	str.WriteString("\n")
	file.WriteString(str.String())

	return instanceStats
}

func (stats *InstanceStats) checkAndInitialiseInstance(inst InstanceID) {
	if _, exists := stats.register[inst]; !exists {
		instanceRegister := make(map[string]int32)
		for i := 0; i < len(stats.orderedKeys); i++ {
			instanceRegister[stats.orderedKeys[i]] = 0
		}
	}
}

func (stats *InstanceStats) RecordOccurrence(inst InstanceID, stat string, count int) {
	stats.checkAndInitialiseInstance(inst)
	stats.register[inst][stat] = stats.register[inst][stat] + count
}

func (stats *InstanceStats) RecordOpened(id InstanceID, time time.Time) {
	stats.checkAndInitialiseInstance(id)
	stats.CommitExecutionComparator.recordOpen(id, time)
}

func (stats *InstanceStats) RecordCommitted(id InstanceID, time time.Time) {
	stats.checkAndInitialiseInstance(id)
	stats.CommitExecutionComparator.recordCommit(id, time)
}

func (stats *InstanceStats) RecordExecuted(id InstanceID, time time.Time) {
	if _, exists := stats.register[id]; !exists {
		panic("Oh no, we have executed without committing apparently! Maybe we just forgot to record it??")
	}
	stats.CommitExecutionComparator.recordExecution(id, time)
}

func (stats *InstanceStats) OutputRecord(id InstanceID) {
	// needs instance and all prev instances to be executed and printed --
	//can rely on the fact that inst i can only call exec if all j < i are also executed
	str := strings.Builder{}
	for i := 0; i < len(stats.orderedKeys); i++ {
		k := stats.orderedKeys[i]
		v := stats.register[id][k]
		str.WriteString(fmt.Sprintf("%d, ", v))
	}
	cmtExecCmpStr := stats.CommitExecutionComparator.outputInstanceTimes(id)
	str.WriteString(cmtExecCmpStr)
	str.WriteString("\n")
	stats.outputFile.WriteString(fmt.Sprintf("%d, %d, %s\n", id.Log, id.Seq, str))
	delete(stats.register, id)
	delete(stats.CommitExecutionComparator.cmdsTimes, id)
}

func (stats *InstanceStats) Close() {
	stats.outputFile.Close()
}
