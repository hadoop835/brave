package brave.cassandra.driver;

import brave.Tagger;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.CaseFormat;
import zipkin.Constants;

/**
 * Provides reasonable defaults for the data contained in cassandra client spans. Subclass to
 * customize, for example, to add tags based on response headers.
 */
public class CassandraClientParser {

  /** Returns the span name of the statement. Defaults to the lower-camel case type name. */
  public String spanName(Statement statement) {
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, statement.getClass().getSimpleName());
  }

  /**
   * Adds any tags based on the statement that will be sent to the server.
   *
   * <p>By default, this adds the {@link CassandraTraceKeys#CASSANDRA_KEYSPACE} and the {@link
   * CassandraTraceKeys#CASSANDRA_QUERY} for bound statements.
   */
  public void requestTags(Statement statement, Tagger tagger) {
    String keyspace = statement.getKeyspace();
    if (keyspace != null) {
      tagger.tag(CassandraTraceKeys.CASSANDRA_KEYSPACE, statement.getKeyspace());
    }
    if (statement instanceof BoundStatement) {
      tagger.tag(CassandraTraceKeys.CASSANDRA_QUERY,
          ((BoundStatement) statement).preparedStatement().getQueryString());
    }
  }

  /** Adds any tags based on the response received from the server. No default. */
  public void responseTags(ResultSet resultSet, Tagger tagger) {
  }

  /**
   * Adds an {@link Constants#ERROR error tag} for a failed request. Defaults to the throwable's
   * message, or the simple name of the throwable's type.
   *
   * @see Constants#ERROR
   */
  // This says error tags for consistency eventhough we only add one
  public void errorTags(Throwable throwable, Tagger tagger) {
    String message = throwable.getMessage();
    tagger.tag(Constants.ERROR, message != null ? message : throwable.getClass().getSimpleName());
  }
}
