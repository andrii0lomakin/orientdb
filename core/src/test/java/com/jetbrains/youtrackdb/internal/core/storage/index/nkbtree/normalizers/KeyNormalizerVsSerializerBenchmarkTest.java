package com.jetbrains.youtrackdb.internal.core.storage.index.nkbtree.normalizers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.openjdk.jmh.results.format.ResultFormatType;

/** Verifies that the manual benchmark writes native JMH CSV results instead of chart output. */
public class KeyNormalizerVsSerializerBenchmarkTest {

  @Test
  public void benchmarkOptionsWriteResultsAsCsvToTheExpectedFile() {
    final var options = KeyNormalizerVsSerializerBenchmark.createOptions();

    assertEquals(ResultFormatType.CSV, options.getResultFormat().get());
    assertTrue(options.getResult().hasValue());
    assertEquals("core/target/normalizerVsSerializer.csv", options.getResult().get());
  }

  @Test
  public void binaryBenchmarkOperationsAcceptEquivalentSampleInput() throws Exception {
    final var benchmark = new KeyNormalizerVsSerializerBenchmark();

    // Exercise the binary serializer and normalizer with the benchmark's representative sample.
    benchmark.binarySerializer();
    benchmark.binaryNormalizer();
  }
}
