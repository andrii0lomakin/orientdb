package com.jetbrains.youtrackdb.internal.core.db;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jetbrains.youtrackdb.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.SchemaShared;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;

/**
 * Shared skeleton for shared-context caches invalidated by metadata changes.
 *
 * <p>The cache owns the Guava storage, the common invalidate timestamp, hit/miss counters, and the
 * {@link MetadataUpdateListener} fan-in. Concrete caches keep only their key/value semantics and
 * copy policy. Counters are lifetime totals (invalidate clears entries, not stats).
 */
public abstract class AbstractMetadataUpdateCache<K, V> implements MetadataUpdateListener {

  protected final int capacity;
  @Nullable protected final Cache<K, V> cache;
  private final AtomicLong lastInvalidation = new AtomicLong(-1);
  private final LongAdder hits = new LongAdder();
  private final LongAdder misses = new LongAdder();

  protected AbstractMetadataUpdateCache(int size) {
    this.capacity = size;
    this.cache = size > 0 ? CacheBuilder.newBuilder().maximumSize(size).build() : null;
  }

  public long getLastInvalidation() {
    return lastInvalidation.get();
  }

  /** Lifetime count of lookups that returned a cached entry. */
  public long getHits() {
    return hits.sum();
  }

  /** Lifetime count of lookups that found no entry (when the cache is enabled). */
  public long getMisses() {
    return misses.sum();
  }

  /** Current number of cached entries, or {@code 0} when the cache is disabled. */
  public long size() {
    return cacheEnabled() ? cache.size() : 0L;
  }

  protected final void recordHit() {
    hits.increment();
  }

  protected final void recordMiss() {
    misses.increment();
  }

  protected final boolean cacheEnabled() {
    return capacity > 0 && cache != null;
  }

  protected final boolean containsKey(K key) {
    return cacheEnabled() && cache.asMap().containsKey(key);
  }

  @Nullable protected final V getCached(K key) {
    return cacheEnabled() ? cache.getIfPresent(key) : null;
  }

  /**
   * Drops one cached entry, leaving every other entry and the invalidate timestamp untouched. Used
   * by a per-entry staleness gate, which must not punish the entries it did not judge.
   */
  protected final void invalidateCached(K key) {
    if (cacheEnabled()) {
      cache.invalidate(key);
    }
  }

  protected final void putCached(K key, V value) {
    if (cacheEnabled()) {
      cache.put(key, value);
    }
  }

  public void invalidate() {
    if (cache != null) {
      cache.invalidateAll();
    }
    lastInvalidation.set(System.nanoTime());
  }

  @Override
  public void onSchemaUpdate(DatabaseSessionEmbedded session, String databaseName,
      SchemaShared schema) {
    invalidate();
  }

  @Override
  public void onIndexManagerUpdate(DatabaseSessionEmbedded session, String databaseName,
      IndexManagerAbstract indexManager) {
    invalidate();
  }

  @Override
  public void onFunctionLibraryUpdate(DatabaseSessionEmbedded session, String databaseName) {
    invalidate();
  }

  @Override
  public void onSequenceLibraryUpdate(DatabaseSessionEmbedded session, String databaseName) {
    invalidate();
  }

  @Override
  public void onStorageConfigurationUpdate(String databaseName, StorageConfiguration update) {
    invalidate();
  }
}
