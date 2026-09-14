/**
 * Copyright (c) 2024-2026 JetBrains s.r.o. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jetbrains.youtrackdb.ycsb.workloads;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.ycsb.Client;
import com.jetbrains.youtrackdb.ycsb.generator.DiscreteGenerator;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

/**
 * Verifies that each workload property file, when merged with
 * workload-common.properties, produces a valid CoreWorkload configuration
 * with the expected operation mix.
 */
public class WorkloadPropertyFilesTest {

  /**
   * Resolves the workloads directory — works both from the ycsb module root
   * (Maven default) and from the repository root.
   */
  private static Path workloadsDir() {
    Path candidate = Path.of("workloads");
    if (candidate.toFile().isDirectory()) {
      return candidate;
    }
    candidate = Path.of("ycsb", "workloads");
    if (candidate.toFile().isDirectory()) {
      return candidate;
    }
    throw new AssertionError(
        "Cannot find workloads directory — expected ./workloads/ or ./ycsb/workloads/");
  }

  /** Loads only common properties (no per-workload overlay). */
  private static Properties loadCommonOnly() throws IOException {
    Path dir = workloadsDir();
    Properties props = new Properties();
    try (FileInputStream common =
        new FileInputStream(dir.resolve("workload-common.properties").toFile())) {
      props.load(common);
    }
    return props;
  }

  /** Loads common properties merged with per-workload properties. */
  private static Properties loadWorkload(String workloadName) throws IOException {
    Path dir = workloadsDir();
    Properties props = new Properties();
    try (FileInputStream common =
        new FileInputStream(dir.resolve("workload-common.properties").toFile())) {
      props.load(common);
    }
    try (FileInputStream wl =
        new FileInputStream(
            dir.resolve("workload-" + workloadName + ".properties").toFile())) {
      props.load(wl);
    }
    return props;
  }

  /**
   * Operation types produced by {@link CoreWorkload#createOperationGenerator}.
   * Must stay in sync with the operation name strings emitted by that method.
   */
  private enum Op {
    READ, UPDATE, INSERT, SCAN, READMODIFYWRITE
  }

  /**
   * Samples the operation generator N times and returns the observed
   * operation distribution as a map of operation type to count.
   */
  private static Map<Op, Integer> sampleOperations(Properties props, int samples) {
    DiscreteGenerator gen = CoreWorkload.createOperationGenerator(props);
    Map<Op, Integer> counts = new EnumMap<>(Op.class);
    for (Op op : Op.values()) {
      counts.put(op, 0);
    }
    for (int i = 0; i < samples; i++) {
      String opName = gen.nextString();
      Op op = Op.valueOf(opName);
      counts.merge(op, 1, Integer::sum);
    }
    return counts;
  }

  /**
   * Asserts that only the expected operations appear in the distribution
   * and that all expected operations are represented.
   */
  private static void assertOnlyOperations(
      Map<Op, Integer> counts, Set<Op> expected, int samples) {
    for (Op op : Op.values()) {
      if (expected.contains(op)) {
        assertTrue(
            "Expected operation " + op + " to appear but it did not",
            counts.get(op) > 0);
      } else {
        assertEquals(
            "Unexpected operation " + op + " appeared in distribution",
            0, (int) counts.get(op));
      }
    }
    int total = counts.values().stream().mapToInt(Integer::intValue).sum();
    assertEquals("Total sample count mismatch", samples, total);
  }

  /** Asserts an observed ratio is within +/-0.03 of the expected proportion. */
  private static void assertRatio(
      String label, double expected, double observed) {
    assertTrue(
        label + " ratio " + observed + " should be within 0.03 of " + expected,
        observed >= expected - 0.03 && observed <= expected + 0.03);
  }

  // ---- Common property verification ----

  @Test
  public void commonPropertiesHaveRequiredFields() throws IOException {
    // Load common properties only (no per-workload overlay) to verify
    // shared settings are not accidentally shadowed by workload files.
    Properties props = loadCommonOnly();

    assertEquals(
        "com.jetbrains.youtrackdb.ycsb.workloads.CoreWorkload",
        props.getProperty(Client.WORKLOAD_PROPERTY));
    assertEquals(
        "com.jetbrains.youtrackdb.ycsb.binding.YouTrackDBYqlClient",
        props.getProperty(Client.DB_PROPERTY));
    assertEquals("3500000", props.getProperty(Client.RECORD_COUNT_PROPERTY));
    assertEquals("1000000", props.getProperty(Client.OPERATION_COUNT_PROPERTY));
    assertEquals("10", props.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY));
    assertEquals("100", props.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY));
    assertEquals("usertable", props.getProperty(CoreWorkload.TABLENAME_PROPERTY));
    assertEquals("zipfian",
        props.getProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY));
    assertEquals("hdrhistogram", props.getProperty("measurementtype"));
    assertEquals("50,95,99,99.9", props.getProperty("hdrhistogram.percentiles"));
  }

  @Test
  public void projectPropertiesContainsMavenFilteredVersion() throws IOException {
    Properties props = new Properties();
    try (InputStream in = getClass().getClassLoader()
        .getResourceAsStream("project.properties")) {
      assertNotNull("project.properties not found on classpath", in);
      props.load(in);
    }
    String version = props.getProperty("version");
    assertNotNull("version property not found", version);
    assertFalse(
        "version should not contain unresolved placeholder: " + version,
        version.contains("${"));
    assertFalse("version should not be empty", version.isBlank());
  }

  // ---- Per-workload operation mix tests ----

  @Test
  public void workloadB_readMostlyWith5PercentUpdate() throws IOException {
    Properties props = loadWorkload("B");
    Map<Op, Integer> counts = sampleOperations(props, 10_000);

    assertOnlyOperations(counts, Set.of(Op.READ, Op.UPDATE), 10_000);

    assertRatio("Read", 0.95, counts.get(Op.READ) / 10_000.0);
    assertRatio("Update", 0.05, counts.get(Op.UPDATE) / 10_000.0);
  }

  @Test
  public void workloadC_readOnly() throws IOException {
    Properties props = loadWorkload("C");
    Map<Op, Integer> counts = sampleOperations(props, 10_000);

    assertOnlyOperations(counts, Set.of(Op.READ), 10_000);
    assertEquals("All operations should be READ", 10_000, (int) counts.get(Op.READ));
  }

  @Test
  public void workloadE_scanHeavyWith5PercentInsert() throws IOException {
    Properties props = loadWorkload("E");
    Map<Op, Integer> counts = sampleOperations(props, 10_000);

    assertOnlyOperations(counts, Set.of(Op.SCAN, Op.INSERT), 10_000);

    assertRatio("Scan", 0.95, counts.get(Op.SCAN) / 10_000.0);
    assertRatio("Insert", 0.05, counts.get(Op.INSERT) / 10_000.0);

    // Verify scan-specific properties
    assertEquals("100", props.getProperty(CoreWorkload.MAX_SCAN_LENGTH_PROPERTY));
    assertEquals(
        "uniform", props.getProperty(CoreWorkload.SCAN_LENGTH_DISTRIBUTION_PROPERTY));
  }

  @Test
  public void workloadW_balancedReadUpdate() throws IOException {
    Properties props = loadWorkload("W");
    Map<Op, Integer> counts = sampleOperations(props, 10_000);

    assertOnlyOperations(counts, Set.of(Op.READ, Op.UPDATE), 10_000);

    assertRatio("Read", 0.50, counts.get(Op.READ) / 10_000.0);
    assertRatio("Update", 0.50, counts.get(Op.UPDATE) / 10_000.0);
  }

  @Test
  public void workloadI_insertBurstWith20PercentRead() throws IOException {
    Properties props = loadWorkload("I");
    Map<Op, Integer> counts = sampleOperations(props, 10_000);

    assertOnlyOperations(counts, Set.of(Op.READ, Op.INSERT), 10_000);

    assertRatio("Insert", 0.80, counts.get(Op.INSERT) / 10_000.0);
    assertRatio("Read", 0.20, counts.get(Op.READ) / 10_000.0);
  }

  @Test
  public void allWorkloadsUseZipfianDistribution() throws IOException {
    for (String name : new String[] {"B", "C", "E", "W", "I"}) {
      Properties props = loadWorkload(name);
      assertEquals(
          "Workload " + name + " should use zipfian distribution",
          "zipfian",
          props.getProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY));
    }
  }

  // ---- Exact property value verification ----

  @Test
  public void allWorkloadProportionValuesAreExact() throws IOException {
    // Verify all five proportions for each workload, including zeros.
    // Zero-value lines are load-bearing: they suppress CoreWorkload defaults
    // (e.g., readproportion defaults to 0.95, updateproportion to 0.05).

    // Workload B: 95% read, 5% update
    Properties b = loadWorkload("B");
    assertEquals("0.95", b.getProperty(CoreWorkload.READ_PROPORTION_PROPERTY));
    assertEquals("0.05", b.getProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY));
    assertEquals("0.0", b.getProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY));
    assertEquals("0.0", b.getProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY));
    assertEquals("0.0",
        b.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY));

    // Workload C: 100% read
    Properties c = loadWorkload("C");
    assertEquals("1.0", c.getProperty(CoreWorkload.READ_PROPORTION_PROPERTY));
    assertEquals("0.0", c.getProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY));
    assertEquals("0.0", c.getProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY));
    assertEquals("0.0", c.getProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY));
    assertEquals("0.0",
        c.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY));

    // Workload E: 95% scan, 5% insert
    Properties e = loadWorkload("E");
    assertEquals("0.0", e.getProperty(CoreWorkload.READ_PROPORTION_PROPERTY));
    assertEquals("0.0", e.getProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY));
    assertEquals("0.05", e.getProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY));
    assertEquals("0.95", e.getProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY));
    assertEquals("0.0",
        e.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY));

    // Workload W: 50% read, 50% update
    Properties w = loadWorkload("W");
    assertEquals("0.5", w.getProperty(CoreWorkload.READ_PROPORTION_PROPERTY));
    assertEquals("0.5", w.getProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY));
    assertEquals("0.0", w.getProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY));
    assertEquals("0.0", w.getProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY));
    assertEquals("0.0",
        w.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY));

    // Workload I: 80% insert, 20% read
    Properties i = loadWorkload("I");
    assertEquals("0.2", i.getProperty(CoreWorkload.READ_PROPORTION_PROPERTY));
    assertEquals("0.0", i.getProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY));
    assertEquals("0.8", i.getProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY));
    assertEquals("0.0", i.getProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY));
    assertEquals("0.0",
        i.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY));
  }

  @Test
  public void allWorkloadProportionsSumToOne() throws IOException {
    // Use the same defaults that CoreWorkload.createOperationGenerator() uses,
    // so a missing proportion line is detected as a sum mismatch rather than
    // silently falling back to "0".
    for (String name : new String[] {"B", "C", "E", "W", "I"}) {
      Properties props = loadWorkload(name);
      double sum =
          Double.parseDouble(
              props.getProperty(CoreWorkload.READ_PROPORTION_PROPERTY,
                  CoreWorkload.READ_PROPORTION_PROPERTY_DEFAULT))
              + Double.parseDouble(
                  props.getProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY,
                      CoreWorkload.UPDATE_PROPORTION_PROPERTY_DEFAULT))
              + Double.parseDouble(
                  props.getProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY,
                      CoreWorkload.INSERT_PROPORTION_PROPERTY_DEFAULT))
              + Double.parseDouble(
                  props.getProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY,
                      CoreWorkload.SCAN_PROPORTION_PROPERTY_DEFAULT))
              + Double.parseDouble(
                  props.getProperty(
                      CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY,
                      CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
      assertEquals(
          "Workload " + name + " proportions should sum to 1.0",
          1.0, sum, 0.001);
    }
  }
}
