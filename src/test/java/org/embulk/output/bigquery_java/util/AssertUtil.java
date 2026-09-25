package org.embulk.output.bigquery_java.util;

import static org.junit.Assert.assertTrue;

// Shared assertion helpers for tests in this project.
public final class AssertUtil {
  private AssertUtil() {}

  // Asserts actual matches regex, with a failure message showing both (plain
  // assertTrue(actual.matches(regex)) only reports true/false).
  public static void assertMatches(String actual, String regex) {
    assertTrue("expected <" + actual + "> to match regex <" + regex + ">", actual.matches(regex));
  }

  // Asserts that `action` throws exactly one instance of `expectedType`, and returns it for
  // further assertions (e.g. on its message). An exception of any other type propagates instead
  // of being swallowed; no exception at all fails the assertion.
  public static <T extends Exception> T assertThrows(Class<T> expectedType, ThrowingRunnable action)
      throws Exception {
    try {
      action.run();
    } catch (Exception e) {
      if (expectedType.isInstance(e)) {
        return expectedType.cast(e);
      }
      throw e;
    }
    throw new AssertionError("expected " + expectedType.getName() + " to be thrown");
  }

  @FunctionalInterface
  public interface ThrowingRunnable {
    void run() throws Exception;
  }
}
