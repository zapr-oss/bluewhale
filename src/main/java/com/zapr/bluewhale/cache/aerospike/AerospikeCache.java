/*
 * Copyright (c) 2017-present, Red Brick Lane Marketing Solutions Pvt. Ltd.
 * All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.zapr.bluewhale.cache.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.zapr.bluewhale.cache.BlueWhaleCache;
import com.zapr.bluewhale.exception.BlueWhaleCacheException;
import com.zapr.bluewhale.exception.BlueWhaleCacheInitializationException;
import com.zapr.bluewhale.exception.BlueWhaleCacheUpdationException;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.NonNull;

public class AerospikeCache<K, V> implements BlueWhaleCache<K, V, AerospikeClient> {

    // No thread safety guarantees here, but will never be an issue
    // as its reference will always change atomically
    private Map<K, V> entries;

    // Delegate which is responsible for populating cache after loading from source
    private AerospikeLoaderDelegate aerospikeLoaderDelegate;

    // Aerospike Query
    private Statement statement;

    // Name of cache
    @Getter
    private String name;

    // QueryPolicy
    @Getter
    private QueryPolicy queryPolicy;

    // Keeps track if cache is initialized or not
    @Getter
    private boolean isCacheInitialized = false;

    /**
     * Intialises a bluewhale cache, which loads from aerospike.
     * Cache is not populated through this function.
     *
     * @param aerospikeLoaderDelegate function which is responsible for populating cache after
     *                                loading from source
     * @param name                    Name of cache
     * @param statement               Aerospike Query
     * @param queryPolicy             QueryPolicy (to be send if want to override client's query
     *                                policy)
     */
    public AerospikeCache(@NonNull AerospikeLoaderDelegate aerospikeLoaderDelegate, @NonNull String name, @NonNull Statement
            statement, QueryPolicy queryPolicy) {
        this.aerospikeLoaderDelegate = aerospikeLoaderDelegate;
        this.name = name;
        this.statement = statement;
        this.queryPolicy = queryPolicy;
        this.entries = new HashMap<K, V>();
        this.isCacheInitialized = false;
    }

    /**
     * Gets the item corresponding to key passed.
     *
     * @param key Key corresponding to the required element.
     */
    @Override
    public V query(K key) {
        return this.entries.get(key);
    }

    /**
     * Builds up the cache and populate according to loaderdelegate, passed in constructor.
     *
     * @param aerospikeClient AerospikeClient through which records will be fetched
     */
    @Override
    public void init(AerospikeClient aerospikeClient) throws BlueWhaleCacheInitializationException {

        if (aerospikeClient == null) {
            throw new BlueWhaleCacheInitializationException("Aerospike Client cannot be null");
        }

        this.entries.clear();

        try {
            buildCache(aerospikeClient, this.entries);
            this.isCacheInitialized = true;
        } catch (BlueWhaleCacheException exception) {
            throw new BlueWhaleCacheInitializationException(exception.getMessage(), exception);
        }

    }

    /**
     * Builds cache
     */
    private void buildCache(AerospikeClient aerospikeClient, Map entries) throws BlueWhaleCacheException {

        RecordSet recordSet = null;

        try {
            recordSet = aerospikeClient.query(this.queryPolicy, this.statement);
            this.aerospikeLoaderDelegate.addEntry(recordSet, entries);

        } catch (AerospikeException aerospikeException) {
            throw new BlueWhaleCacheException("Aerospike Exception while querying from aerospike for cache " + this.name,
                    aerospikeException);
        } finally {
            if (recordSet != null) {
                recordSet.close();
            }
        }
    }

    /**
     * Cases where atomicity of cache update is not guaranteed,
     * Update cache through it (not-recommended for frequent usage)
     *
     * @param aerospikeClient AerospikeClient through which records will be fetched
     */
    @Override
    public void unsafeUpdate(AerospikeClient aerospikeClient) throws BlueWhaleCacheUpdationException {

        if (aerospikeClient == null) {
            throw new BlueWhaleCacheUpdationException("Aerospike Client cannot be null");
        }

        Map entries = new HashMap<>();

        try {
            buildCache(aerospikeClient, entries);
            this.isCacheInitialized = true;
        } catch (BlueWhaleCacheException exception) {
            throw new BlueWhaleCacheUpdationException(exception.getMessage(), exception);
        }

        synchronized (this) {
            this.entries = entries;
        }
    }

    /**
     * Gets all entries of cache
     */
    @Override
    public Map<K, V> getAll() throws BlueWhaleCacheUpdationException {
        return this.entries;
    }

    /**
     * Gets size of cache
     */
    @Override
    public int getSize() {
        return this.entries.size();
    }
}
