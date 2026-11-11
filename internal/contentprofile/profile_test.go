package contentprofile

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"
)

func TestNewSnapshotForWorkspaceUsesMatchingProviderIdentity(t *testing.T) {
	tests := []struct {
		name        string
		workspaceID string
		provider    string
		region      string
		model       string
		profile     string
	}{
		{
			name:        "local demo",
			workspaceID: LocalDemoWorkspace,
			provider:    "local-demo",
			region:      "local",
			model:       LocalDemoModel,
			profile:     LocalDemoProfile,
		},
		{
			name:        "bailian workspace",
			workspaceID: "interview-workspace",
			provider:    "aliyun-bailian",
			region:      "cn-beijing",
			model:       Model,
			profile:     ProfileVersion,
		},
	}

	var snapshots [][]byte
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, hash, err := NewSnapshotForWorkspace(test.workspaceID)
			if err != nil {
				t.Fatalf("create profile snapshot: %v", err)
			}
			var snapshot Snapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				t.Fatalf("decode profile snapshot: %v", err)
			}
			if snapshot.Provider != test.provider || snapshot.Region != test.region ||
				snapshot.WorkspaceID != test.workspaceID || snapshot.Model != test.model ||
				snapshot.Profile != test.profile || snapshot.EnableThinking {
				t.Fatalf("unexpected profile snapshot: %+v", snapshot)
			}
			expectedHash := sha256.Sum256(raw)
			if !bytes.Equal(hash, expectedHash[:]) {
				t.Fatalf("profile hash does not match serialized snapshot")
			}
			snapshots = append(snapshots, raw)
		})
	}
	if bytes.Equal(snapshots[0], snapshots[1]) {
		t.Fatal("local-demo and Bailian workspaces must persist distinct profiles")
	}
}

func TestNewSnapshotForWorkspaceRejectsOversizedWorkspaceID(t *testing.T) {
	workspaceID := string(bytes.Repeat([]byte("x"), 129))
	if _, _, err := NewSnapshotForWorkspace(workspaceID); err == nil {
		t.Fatal("expected oversized workspace ID to be rejected")
	}
}
