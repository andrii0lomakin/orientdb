package com.jetbrains.youtrackdb.api.gremlin;

import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;
import com.jetbrains.youtrackdb.internal.core.exception.BaseException;
import com.jetbrains.youtrackdb.internal.core.exception.DatabaseException;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBTransaction;
import com.jetbrains.youtrackdb.internal.core.gremlin.service.GqlService;
import com.jetbrains.youtrackdb.internal.core.gremlin.service.YTDBCommandService;
import com.jetbrains.youtrackdb.internal.core.gremlin.service.YTDBFullBackupService;
import com.jetbrains.youtrackdb.internal.core.gremlin.service.YTDBGraphUuidService;
import com.jetbrains.youtrackdb.internal.core.gremlin.service.YTDBIncrementalBackupService;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.structure.Graph;

@SuppressWarnings("unused")
public class YTDBGraphTraversalSourceDSL extends GraphTraversalSource {

  public YTDBGraphTraversalSourceDSL(Graph graph,
      TraversalStrategies traversalStrategies) {
    super(graph, traversalStrategies);
  }

  public YTDBGraphTraversalSourceDSL(Graph graph) {
    super(graph);
  }

  public YTDBGraphTraversalSourceDSL(
      RemoteConnection connection) {
    super(connection);
  }

  public YTDBGraphTraversalSource with(final YTDBQueryConfigParam key, final Object value) {
    if (!key.accepts(value)) {
      throw new IllegalArgumentException(
          "Invalid value '" + value + "' for parameter '" + key.name() + "'. "
              + key.validationDescription());
    }
    return (YTDBGraphTraversalSource) with(key.name(), key.normalizeValue(value));
  }

  public YTDBGraphTraversalSource with(final YTDBQueryConfigParam key) {
    return with(key, true);
  }

  /// Start a new transaction if it is not yet started and executes passed in code in it.
  ///
  /// If a transaction is already started, executes passed in code in it. In case of exception,
  /// rolls back the transaction and commits the changes if the transaction was started by this
  /// method.
  public <X extends Exception> void executeInTx(
      @Nonnull FailableConsumer<YTDBGraphTraversalSource, X> code) throws X {
    YTDBTransaction.executeInTX(code, (YTDBGraphTraversalSource) this);
  }

  /// Start a new transaction if it is not yet started and executes passed in code in it.
  ///
  /// If a transaction is already started, executes passed in code in it. In case of exception,
  /// rolls back the transaction and commits the changes if the transaction was started by this
  /// method.
  ///
  /// Unlike {@link #executeInTx(FailableConsumer)} also iterates over the returned
  /// [YTDBGraphTraversal] triggering its execution.
  public <X extends Exception> void autoExecuteInTx(
      @Nonnull FailableFunction<YTDBGraphTraversalSource, YTDBGraphTraversal<?, ?>, X> code)
      throws X {
    YTDBTransaction.executeInTX(code, (YTDBGraphTraversalSource) this);
  }

  /// Start a new transaction if it is not yet started and executes passed in code in it and then
  /// returns the result of the code execution.
  ///
  /// If a transaction is already started, executes passed in code in it. In case of exception,
  /// rolls back the transaction and commits the changes if the transaction was started by this
  /// method.
  public <X extends Exception, R> R computeInTx(
      @Nonnull FailableFunction<YTDBGraphTraversalSource, R, X> code) throws X {
    return YTDBTransaction.computeInTx(code, (YTDBGraphTraversalSource) this);
  }

  /// Execute a generic YouTrackDB command immediately. The command is executed eagerly - no need to
  /// call .iterate().
  ///
  /// @param command The command to execute.
  public void command(@Nonnull String command) {
    this.call(
        YTDBCommandService.NAME, Map.of(
            YTDBCommandService.COMMAND, command,
            YTDBCommandService.ARGUMENTS, Map.of()))
        .iterate();
  }

  /// Execute a generic parameterized YouTrackDB command immediately. The command is executed
  /// eagerly - no need to call .iterate().
  ///
  /// @param command   The command to execute.
  /// @param keyValues Alternating key/value pairs for command parameters (key1, value1, key2,
  ///                  value2, ...).
  public void command(@Nonnull String command, @Nonnull Object... keyValues) {
    var arguments = processKeyValueArguments(keyValues);
    this.call(
        YTDBCommandService.NAME, Map.of(
            YTDBCommandService.COMMAND, command,
            YTDBCommandService.ARGUMENTS, arguments))
        .iterate();
  }

  /// Execute a generic YouTrackDB YQL command. Returns a lazy traversal that can be chained. Users
  /// must call .iterate() or another terminal operation to execute the command.
  ///
  /// @param command The YQL command to execute.
  /// @return A traversal that can be chained with other steps.
  public YTDBGraphTraversal<Object, Object> yql(@Nonnull String command) {
    return call(
        YTDBCommandService.YQL_NAME,
        Map.of(
            YTDBCommandService.COMMAND, command,
            YTDBCommandService.ARGUMENTS, Map.of()));
  }

  /// Execute a generic parameterized YouTrackDB YQL command. Returns a lazy traversal. Users must
  /// call .iterate() or another terminal operation to execute the command.
  ///
  /// @param command   The YQL command to execute.
  /// @param keyValues Alternating key/value pairs for command parameters (key1, value1, key2,
  ///                  value2, ...).
  /// @return A traversal that can be chained with other steps.
  public YTDBGraphTraversal<Object, Object> yql(@Nonnull String command,
      @Nonnull Object... keyValues) {
    var arguments = processKeyValueArguments(keyValues);
    return call(
        YTDBCommandService.YQL_NAME,
        Map.of(
            YTDBCommandService.COMMAND, command,
            YTDBCommandService.ARGUMENTS, arguments));
  }

  /// Override required because the {@code GremlinDslProcessor} does not generate a
  /// {@code call()} override in the traversal source. Without this, {@code call()} resolves
  /// to {@link GraphTraversalSource#call(String, Map)} which returns a
  /// {@code DefaultGraphTraversal} instead of {@code YTDBGraphTraversal}.
  @Override
  public <S> YTDBGraphTraversal<S, S> call(String service, Map params) {
    var clone = (YTDBGraphTraversalSource) this.clone();
    clone.getBytecode().addStep(GraphTraversal.Symbols.call, service, params);
    var traversal = new DefaultYTDBGraphTraversal<>(clone);
    return (YTDBGraphTraversal<S, S>) traversal.asAdmin()
        .addStep(new CallStep<>(traversal, true, service, params));
  }

  private static Map<Object, Object> processKeyValueArguments(@Nonnull Object[] keyValues) {
    if (keyValues.length % 2 != 0) {
      throw new IllegalArgumentException("keyValues must be an even number of arguments "
          + "(key/value pairs)");
    }
    var arguments = new HashMap<>();
    for (var i = 0; i < keyValues.length; i += 2) {
      arguments.put(keyValues[i], keyValues[i + 1]);
    }
    return arguments;
  }

  /// Execute a GQL (Graph Query Language) query.
  ///
  /// Returns a traversal of result maps where each map contains variable bindings.
  /// For example, `MATCH (a:Person)` produces maps with key "a" bound to matched vertices.
  ///
  /// @param query The GQL query to execute.
  /// @return A traversal of result maps.
  public YTDBGraphTraversal<Object, Object> gql(@Nonnull String query) {
    return gql(query, Map.of());
  }

  /// Execute a parameterized GQL (Graph Query Language) query.
  ///
  /// Returns a traversal of result maps where each map contains variable bindings.
  /// For example, `MATCH (a:Person)` produces maps with key "a" bound to matched vertices.
  ///
  /// @param query     The GQL query to execute.
  /// @param arguments The arguments to pass to the query.
  /// @return A traversal of result maps.
  public YTDBGraphTraversal<Object, Object> gql(
      @Nonnull String query, @Nonnull Map<?, ?> arguments) {
    return call(
        GqlService.NAME, Map.of(
            GqlService.QUERY, query,
            GqlService.ARGUMENTS, arguments));
  }

  /// Execute a parameterized GQL (Graph Query Language) query with varargs.
  ///
  /// Returns a traversal of result maps where each map contains variable bindings.
  /// For example, `MATCH (a:Person) WHERE a.name = $name` with parameters "name", "Maria"
  /// produces maps with key "a" bound to matched vertices where name is "Maria".
  ///
  /// @param query     The GQL query to execute.
  /// @param keyValues Alternating key/value pairs for query parameters (key1, value1, key2,
  ///                  value2, ...).
  /// @return A traversal of result maps.
  public YTDBGraphTraversal<Object, Object> gql(
      @Nonnull String query, @Nonnull Object... keyValues) {
    var arguments = processKeyValueArguments(keyValues);
    return gql(query, arguments);
  }

  /// Performs backup of database content to the selected folder.
  ///
  /// During the first backup full content of the database will be copied into the directory,
  /// otherwise only changes after the last backup in the same folder will be copied.
  ///
  /// @param path Path to the backup folder.
  /// @return The name of the last backup file.
  public String backup(final Path path) {
    var clone = (YTDBGraphTraversalSource) this.clone();
    var params = Map.of(YTDBIncrementalBackupService.PATH, path.toString());
    clone.getBytecode().addStep("call", YTDBIncrementalBackupService.NAME, params);

    try (var traversal = new DefaultYTDBGraphTraversal<>(clone)) {
      return (String) traversal.addStep(
          new CallStep<>(traversal, true, YTDBIncrementalBackupService.NAME, params))
          .next();
    } catch (Exception e) {
      throw BaseException.wrapException(new DatabaseException("Error during incremental backup"), e,
          graph.toString());
    }
  }

  /// Performs backup of database content to the selected folder.
  ///
  /// If the incremental backup is present in the folder, it will be overwritten.
  ///
  /// @param path Path to the backup folder.
  /// @return The name of the backup file.
  public String fullBackup(final Path path) {
    var clone = (YTDBGraphTraversalSource) this.clone();
    var params = Map.of(YTDBFullBackupService.PATH, path.toString());
    clone.getBytecode().addStep("call", YTDBFullBackupService.NAME, params);

    try (var traversal = new DefaultYTDBGraphTraversal<>(clone)) {
      return (String) traversal.addStep(
          new CallStep<>(traversal, true, YTDBFullBackupService.NAME, params))
          .next();
    } catch (Exception e) {
      throw BaseException.wrapException(new DatabaseException("Error during full backup"), e,
          graph.toString());
    }
  }

  /// Returns [UUID] generated during the creation of the database. Each DB instance his unique
  /// [UUID] identifier.
  ///
  /// It is used during the generation of backups and later restores to identify the database
  /// instance.
  public UUID uuid() {
    var clone = (YTDBGraphTraversalSource) this.clone();
    clone.getBytecode().addStep("call", YTDBGraphUuidService.NAME, Map.of());

    try (var traversal = new DefaultYTDBGraphTraversal<>(clone)) {
      return UUID.fromString((String) traversal.addStep(
          new CallStep<>(traversal, true, YTDBGraphUuidService.NAME, Map.of()))
          .next());
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException("Error during retrieving database uuid"), e,
          graph.toString());
    }
  }

  @Override
  public <S> YTDBGraphTraversal<S, S> io(final String file) {
    var clone = (YTDBGraphTraversalSource) this.clone();
    clone.getBytecode().addStep("io", file);
    var traversal = new DefaultYTDBGraphTraversal<>(clone);
    traversal.addStep(new IoStep<>(traversal, file));
    //noinspection unchecked
    return (YTDBGraphTraversal<S, S>) traversal;
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (Exception e) {
      throw new RuntimeException("Error during closing of GraphTraversalSource", e);
    }
  }
}
