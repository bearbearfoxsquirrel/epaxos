package CommitExecutionComparator

import (
	"fmt"
	"os"
	"time"
)

type InstanceID struct {
	Log int32
	Seq int32
}

type TimeTuple struct {
	Commit  time.Time
	Execute time.Time
}

type CommitExecutionComparator struct {
	outputFile *os.File
	cmdsTimes  map[InstanceID]TimeTuple
}

func CommitExecutionComparatorNew(outputLoc string) *CommitExecutionComparator {
	file, _ := os.Create(outputLoc)
	return &CommitExecutionComparator{
		outputFile: file,
		cmdsTimes:  make(map[InstanceID]TimeTuple),
	}
}

func (c CommitExecutionComparator) RecordCommit(id InstanceID, time time.Time) {
	timeTup, _ := c.cmdsTimes[id]

	if !timeTup.Commit.IsZero() {
		return
	}

	c.cmdsTimes[id] = TimeTuple{Commit: time}
}

func (c CommitExecutionComparator) RecordExecution(id InstanceID, time time.Time) {
	timeTup, exists := c.cmdsTimes[id]
	if !exists {
		panic("somehow recording execution when not first recorded commiting")
	}

	if !timeTup.Execute.IsZero() {
		return
	}

	c.cmdsTimes[id] = TimeTuple{
		Commit:  timeTup.Commit,
		Execute: time,
	}
	c.Output(id)
}

func (c CommitExecutionComparator) Output(id InstanceID) {
	timeTup, exists := c.cmdsTimes[id]
	if !exists {
		panic("cannot output a command with no record of commit or execution")
	}

	diff := timeTup.Execute.Sub(timeTup.Commit)

	c.outputFile.WriteString(fmt.Sprintf("%d, %d, %d, %d, %d\n", id.Log, id.Seq, timeTup.Commit.UnixNano(), timeTup.Execute.UnixNano(), diff.Microseconds()))
}
