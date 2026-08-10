package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.notify.artifact.worker.JobStateMachines;
import org.junit.jupiter.api.Test;

class StateMachineTest {
  @Test
  void rejectsInvalidTransitionsAndKeepsHistory() {
    var sm = new JobStateMachines.Index();
    sm.transition(JobStateMachines.State.VALIDATING, "claimed");
    assertEquals(1, sm.history().size());
    assertThrows(
        IllegalStateException.class, () -> sm.transition(JobStateMachines.State.COMPLETED, "skip"));
  }
}
