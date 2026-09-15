package com.jetbrains.youtrackdb.internal.core.storage.index.nkbtree.normalizers;

import static com.jetbrains.youtrackdb.internal.common.serialization.types.LongSerializer.LONG_SIZE;

import com.jetbrains.youtrackdb.internal.common.serialization.types.BinaryTypeSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.BooleanSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.DateSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.DateTimeSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.DecimalSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.DoubleSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.FloatSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.ShortSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.StringSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.UTF8Serializer;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.Collator;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(0)
public class KeyNormalizerVsSerializerBenchmark {

  private final BinarySerializerFactory serializerFactory = BinarySerializerFactory.create(
      BinarySerializerFactory.currentBinaryFormatVersion());

  public static void main(String[] args) throws RunnerException {
    new Runner(createOptions()).run();
  }

  static Options createOptions() {
    return new OptionsBuilder()
        .include("KeyNormalizerVsSerializerBenchmark.*")
        .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
        .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
        .result("core/target/normalizerVsSerializer.csv")
        .resultFormat(ResultFormatType.CSV)
        .build();
  }

  @Setup(Level.Iteration)
  public void setup() {
  }

  @Benchmark
  public void booleanSerializer() {
    final var serializer = new BooleanSerializer();
    serializer.serialize(true, serializerFactory, new byte[1], 0);
  }

  @Benchmark
  public void booleanNormalizer() throws Exception {
    final var normalizer = new BooleanKeyNormalizer();
    normalizer.execute(true, 0);
  }

  @Benchmark
  public void byteSerializer() {
    final var serializer = new ByteSerializer();
    serializer.serialize((byte) 3, serializerFactory, new byte[1], 0);
  }

  @Benchmark
  public void byteNormalizer() throws Exception {
    final var normalizer = new ByteKeyNormalizer();
    normalizer.execute((byte) 3, 0);
  }

  @Benchmark
  public void integerSerializer() {
    final var serializer = new IntegerSerializer();
    serializer.serialize(5, serializerFactory, new byte[4], 0);
  }

  @Benchmark
  public void integerNormalizer() throws Exception {
    final var normalizer = new IntegerKeyNormalizer();
    normalizer.execute(5, 0);
  }

  @Benchmark
  public void floatSerializer() {
    final var serializer = new FloatSerializer();
    serializer.serialize(1.5f, serializerFactory, new byte[4], 0);
  }

  @Benchmark
  public void floatNormalizer() throws Exception {
    final var normalizer = new FloatKeyNormalizer();
    normalizer.execute(1.5f, 0);
  }

  @Benchmark
  public void doubleSerializer() {
    final var serializer = new DoubleSerializer();
    serializer.serialize(1.5d, serializerFactory, new byte[8], 0);
  }

  @Benchmark
  public void doubleNormalizer() throws Exception {
    final var normalizer = new DoubleKeyNormalizer();
    normalizer.execute(1.5d, 0);
  }

  @Benchmark
  public void shortSerializer() {
    final var serializer = new ShortSerializer();
    serializer.serialize((short) 3, serializerFactory, new byte[2], 0);
  }

  @Benchmark
  public void shortNormalizer() throws Exception {
    final var normalizer = new ShortKeyNormalizer();
    normalizer.execute((short) 3, 0);
  }

  @Benchmark
  public void longSerializer() {
    final var serializer = new LongSerializer();
    serializer.serialize(5L, serializerFactory, new byte[LONG_SIZE], 0);
  }

  @Benchmark
  public void longNormalizer() throws Exception {
    final var normalizer = new LongKeyNormalizer();
    normalizer.execute(5L, 0);
  }

  @Benchmark
  public void stringSerializer() {
    final var serializer = new StringSerializer();
    serializer.serialize("abcd", serializerFactory, new byte[16], 0);
  }

  @Benchmark
  public void stringUtf8Serializer() {
    final var serializer = new UTF8Serializer();
    serializer.serialize("abcd", serializerFactory, new byte[16], 0);
  }

  @Benchmark
  public void stringNormalizer() throws Exception {
    final var normalizer = new StringKeyNormalizer();
    normalizer.execute("abcd", Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void binarySerializer() {
    final var serializer = new BinaryTypeSerializer();
    final var binary = new byte[] {1, 2, 3, 4, 5, 6};
    serializer.serialize(binary, serializerFactory,
        new byte[binary.length + IntegerSerializer.INT_SIZE], 0);
  }

  @Benchmark
  public void binaryNormalizer() throws Exception {
    final var normalizer = new BinaryKeyNormalizer();
    final var binary = new byte[] {1, 2, 3, 4, 5, 6};
    normalizer.execute(binary, 0);
  }

  @Benchmark
  public void dateSerializer() {
    final var serializer = new DateSerializer();
    final var date = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    serializer.serialize(date, serializerFactory, new byte[LONG_SIZE], 0);
  }

  @Benchmark
  public void dateNormalizer() throws Exception {
    final var normalizer = new DateKeyNormalizer();
    final var date = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    normalizer.execute(date, 0);
  }

  @Benchmark
  public void dateTimeSerializer() {
    final var serializer = new DateTimeSerializer();
    final var ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final var date = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    serializer.serialize(date, serializerFactory, new byte[LONG_SIZE], 0);
  }

  @Benchmark
  public void dateTimeNormalizer() throws Exception {
    final var normalizer = new DateKeyNormalizer();
    final var ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final var date = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    normalizer.execute(date, 0);
  }

  @Benchmark
  public void decimalSerializer() {
    final var serializer = new DecimalSerializer();
    serializer.serialize(new BigDecimal(new BigInteger("20"), 2), serializerFactory, new byte[9],
        0);
  }

  @Benchmark
  public void decimalNormalizer() throws Exception {
    final var normalizer = new DecimalKeyNormalizer();
    normalizer.execute(new BigDecimal(new BigInteger("20"), 2), 0);
  }
}
