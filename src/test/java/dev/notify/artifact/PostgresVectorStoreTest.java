package dev.notify.artifact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.notify.artifact.store.VectorStore;
import dev.notify.artifact.store.postgres.PostgresVectorStore;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

class PostgresVectorStoreTest {
  @Test
  void buildsTenantFirstVectorQueryWithParameterizedMetadataFilters() {
    JdbcTemplate jdbc = org.mockito.Mockito.mock(JdbcTemplate.class);
    when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());
    PostgresVectorStore store = new PostgresVectorStore(jdbc, new ObjectMapper(), 3);

    store.search(
        "tenant-a",
        new float[] {1, 0, 0},
        5,
        new VectorStore.SearchFilter(
            List.of("application/pdf", "image/png"), List.of("production")));

    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
    org.mockito.Mockito.verify(jdbc)
        .query(sql.capture(), any(RowMapper.class), arguments.capture());
    assertTrue(sql.getValue().contains("WHERE c.tenant_id = ?"));
    assertTrue(sql.getValue().contains("a.media_type IN (?,?)"));
    assertTrue(sql.getValue().contains("COALESCE(a.tags_csv, '')"));
    assertEquals("tenant-a", arguments.getValue()[1]);
  }

  @Test
  void rejectsWrongVectorDimensionsBeforeDatabaseAccess() {
    JdbcTemplate jdbc = org.mockito.Mockito.mock(JdbcTemplate.class);
    PostgresVectorStore store = new PostgresVectorStore(jdbc, new ObjectMapper(), 3);

    assertThrows(
        IllegalArgumentException.class,
        () -> store.search("tenant-a", new float[] {1, 2}, 5, VectorStore.SearchFilter.none()));

    verifyNoInteractions(jdbc);
  }
}
