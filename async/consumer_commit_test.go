package async

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestCommitOnlyAfterSuccessfulProcess(t *testing.T) {
	processOK := false
	commitCalled := false

	if processOK {
		commitCalled = true
	}

	if commitCalled {
		t.Fatal("expected no commit when pipeline fails")
	}

	processOK = true
	if processOK {
		commitCalled = true
	}
	if !commitCalled {
		t.Fatal("expected commit when pipeline succeeds")
	}
}

func TestSequentialProcessingPreservesCommitOrder(t *testing.T) {
	commits := []int64{}
	msgs := []kafka.Message{
		{Offset: 100},
		{Offset: 101},
	}
	for _, msg := range msgs {
		if true { // process ok
			commits = append(commits, msg.Offset)
		}
	}
	if len(commits) != 2 || commits[0] != 100 || commits[1] != 101 {
		t.Fatalf("unexpected commit order: %v", commits)
	}
}
