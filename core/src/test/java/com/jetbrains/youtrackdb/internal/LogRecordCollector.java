package com.jetbrains.youtrackdb.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Captures the messages one class logs during a test.
 *
 * <p>{@code LogManager} names its logger after the requester class, and the test scope binds SLF4J
 * to the platform logging backend. Attaching a handler to the logger of that name therefore sees
 * every record the class emits.
 *
 * <p>Close the collector to detach the handler and restore the logger level. The collector is meant
 * for a try-with-resources block inside a single test method.
 */
public final class LogRecordCollector implements AutoCloseable {

  private final Logger logger;
  private final Handler handler;
  private final Level previousLevel;
  private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

  private LogRecordCollector(Class<?> requester) {
    logger = Logger.getLogger(requester.getName());
    previousLevel = logger.getLevel();
    logger.setLevel(Level.ALL);
    handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            messages.add(record.getLevel().getName() + " " + record.getMessage());
          }

          @Override
          public void flush() {
          }

          @Override
          public void close() {
          }
        };
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
  }

  /** Starts capturing the records logged by {@code requester}. */
  public static LogRecordCollector attachTo(Class<?> requester) {
    return new LogRecordCollector(requester);
  }

  /** Returns a snapshot of the captured messages, each prefixed by its level name. */
  public List<String> messages() {
    synchronized (messages) {
      return List.copyOf(messages);
    }
  }

  /** Reports whether some captured warning contains every one of {@code fragments}. */
  public boolean warnedWithAll(String... fragments) {
    for (var message : messages()) {
      if (!message.startsWith("WARNING")) {
        continue;
      }
      var complete = true;
      for (var fragment : fragments) {
        if (!message.contains(fragment)) {
          complete = false;
          break;
        }
      }
      if (complete) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() {
    logger.removeHandler(handler);
    logger.setLevel(previousLevel);
  }
}
