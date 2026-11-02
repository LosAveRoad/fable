//go:build e2e

package e2e

import (
	"encoding/json"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTopologyEnvironmentRejectsAmbientFableSettings(t *testing.T) {
	ambient := []string{"PATH=/bin", "FABLE_LOCAL_TOPIC=foreign", "FABLE_LOCAL_DLQ_TOPIC=foreign-dlq", "FABLE_LOCAL_GROUP=foreign-group", "FABLE_REDIS_PASSWORD=foreign", "FABLE_LOGIC_TEST_MYSQL_DSN=foreign"}
	env := topologyEnvironment(ambient, []string{"FABLE_LOCAL_TOPIC=owned", "FABLE_LOCAL_DLQ_TOPIC=owned-dlq", "FABLE_LOCAL_GROUP=owned-group"})
	if strings.Contains(strings.Join(env, "\n"), "foreign") {
		t.Fatalf("ambient FABLE configuration escaped isolation: %v", env)
	}
	if strings.Join(env, "\n") != "PATH=/bin\nFABLE_LOCAL_TOPIC=owned\nFABLE_LOCAL_DLQ_TOPIC=owned-dlq\nFABLE_LOCAL_GROUP=owned-group" {
		t.Fatal(env)
	}
}

func TestCleanupRejectsForeignResourcesBeforeAnyDeletion(t *testing.T) {
	f := &topology{}
	f.owned.RunID = "123456789abc"
	f.owned.Database.Name = "fable_e2e_123456789abc"
	f.owned.Redis.Prefix = "fable:local:123456789abc:"
	f.owned.Kafka.Topic = "fable.push.v1.e2e_123456789abc"
	f.owned.Kafka.DLQ = "fable.push.dlq.v1.e2e_123456789abc"
	f.owned.Kafka.Group = "fable-push-job-v1.e2e_123456789abc"
	f.owned.Kafka.Brokers = []string{"127.0.0.1:39092"}
	for _, corrupt := range []func(*manifest){
		func(m *manifest) { m.Database.Name = "fable_e2e_foreign" },
		func(m *manifest) { m.Redis.Prefix = "fable:local:foreign:" },
		func(m *manifest) { m.Kafka.Topic = "foreign" },
		func(m *manifest) { m.Kafka.DLQ = "foreign" },
		func(m *manifest) { m.Kafka.Group = "foreign" },
		func(m *manifest) { m.Kafka.Brokers = []string{"127.0.0.1:9092"} },
		func(m *manifest) { m.RunID = "foreign" },
	} {
		f.state = f.owned
		corrupt(&f.state)
		deletes := 0
		if err := f.withOwnedResources(func() { deletes++ }); err == nil || deletes != 0 {
			t.Fatalf("foreign cleanup admitted: error=%v deletes=%d", err, deletes)
		}
	}
	f.state = f.owned
	deletes := 0
	if err := f.withOwnedResources(func() { deletes++ }); err != nil || deletes != 1 {
		t.Fatalf("owned cleanup refused: %v %d", err, deletes)
	}
}

func TestPollutedEnvironmentCannotSelectForeignTopologyResources(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Clean(filepath.Join(cwd, "../.."))
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	runtime := filepath.Join(t.TempDir(), "runtime")
	owned := []string{"FABLE_LOCAL_RUNTIME_DIR=" + runtime, "FABLE_LOCAL_EXTERNAL=1", "FABLE_LOCAL_DATABASE=fable_e2e_" + suffix, "FABLE_LOCAL_TOPIC=fable.push.v1.e2e_" + suffix, "FABLE_LOCAL_DLQ_TOPIC=fable.push.dlq.v1.e2e_" + suffix, "FABLE_LOCAL_GROUP=fable-push-job-v1.e2e_" + suffix, "FABLE_KAFKA_BROKERS=127.0.0.1:39092"}
	polluted := append(os.Environ(), "FABLE_LOCAL_TOPIC=fable.push.v1.foreign", "FABLE_LOCAL_DLQ_TOPIC=fable.push.dlq.v1.foreign", "FABLE_LOCAL_GROUP=fable-push-job-v1.foreign", "FABLE_LOCAL_DATABASE=fable_e2e_foreign", "FABLE_REDIS_PASSWORD=foreign-secret")
	command := exec.Command("python3", "-c", "import sys; from pathlib import Path; sys.path.insert(0,sys.argv[1]); import manage; manage.configure(Path(sys.argv[2]),0,True)", filepath.Join(root, "scripts/local"), runtime)
	command.Env = topologyEnvironment(polluted, owned)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("configure: %v %s", err, output)
	}
	raw, err := os.ReadFile(filepath.Join(runtime, "private.json"))
	if err != nil {
		t.Fatal(err)
	}
	var selected manifest
	if err = json.Unmarshal(raw, &selected); err != nil {
		t.Fatal(err)
	}
	if selected.Database.Name != "fable_e2e_"+suffix || selected.Kafka.Topic != "fable.push.v1.e2e_"+suffix || selected.Kafka.DLQ != "fable.push.dlq.v1.e2e_"+suffix || selected.Kafka.Group != "fable-push-job-v1.e2e_"+suffix || strings.Contains(string(raw), "foreign") {
		t.Fatal("ambient foreign resource selected")
	}
	f := &topology{state: selected, owned: selected}
	deleted := 0
	if err := f.withOwnedResources(func() { deleted++ }); err != nil || deleted != 1 {
		t.Fatal("owned deletion boundary refused", err, deleted)
	}
	f.state.Kafka.Topic = "fable.push.v1.foreign"
	if err := f.withOwnedResources(func() { deleted++ }); err == nil || deleted != 1 {
		t.Fatal("foreign deletion reached boundary", err, deleted)
	}
}
