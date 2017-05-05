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

package com.zapr.bluewhale.cache.sqlDB;

import com.zapr.bluewhale.cache.BlueWhaleCache;
import com.zapr.bluewhale.exception.BlueWhaleCacheException;
import com.zapr.bluewhale.exception.BlueWhaleCacheInitializationException;
import com.zapr.bluewhale.exception.BlueWhaleCacheUpdationException;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DBBlueWhaleCache<K, V> implements BlueWhaleCache<K, V, Connection> {

    @Getter
    private final boolean isCacheUpdatable;
    // refreshInterval in seconds, used only in case of updatable caches
    @Getter
    private final int refreshInterval;
    // No thread safety guarantees here, but will never be an issue
    // as its reference will always change atomically
    private HashMap<K, V> entries;
    // delegate who exposes actual implementation of adding entries in cache
    @Getter
    private DBLoaderDelegate dbLoaderDelegate;
    @Getter
    private boolean isCacheInitialized = false;
    @Getter
    private String query;

    @Getter
    private String name;

    @Getter
    private Timestamp lastLoadTimeStamp;

    public DBBlueWhaleCache(DBLoaderDelegate dbLoaderDelegate, String name, String query,
                            boolean isCacheUpdatable, int refreshInterval) {
        this.dbLoaderDelegate = dbLoaderDelegate;
        this.name = name;
        this.query = query;
        this.isCacheUpdatable = isCacheUpdatable;
        this.entries = new HashMap<>();
        this.lastLoadTimeStamp = new Timestamp(1000L); //set initial timsestamp to a second Post epoc
        this.refreshInterval = refreshInterval;
    }

    public V query(K key) {
        return entries.get(key);
    }

    public void init(Connection dbConnection)
            throws BlueWhaleCacheInitializationException {

        if (StringUtils.isEmpty(query) || null == dbConnection) {
            throw new BlueWhaleCacheInitializationException("Invalid query!!");
        }

        try {
            Timestamp initTimeStamp = new Timestamp(System.currentTimeMillis());
            if (this.isCacheUpdatable()) {
                buildUpdatableDbBackedCache(dbConnection, this.entries, true);
            } else {
                buildNonUpdatableDbBackedCache(dbConnection, this.entries);
            }
            this.lastLoadTimeStamp = initTimeStamp;
        } catch (BlueWhaleCacheException e) {
            throw new BlueWhaleCacheInitializationException("Init failed!!", e);
        }

        isCacheInitialized = true;
    }

    /**
     * refreshCache assumes that client has taken care to avoid consistent state within a
     * transaction
     *
     * @param dbConnection: Connection to Database
     */
    public void refresh(Connection dbConnection)
            throws BlueWhaleCacheInitializationException {

        if (StringUtils.isEmpty(query) || null == dbConnection || !isCacheUpdatable()) {
            throw new BlueWhaleCacheInitializationException("refresh failed due to unmet preconditions!!");
        }

        try {
            Timestamp refreshAttemptTimeStamp = new Timestamp(System.currentTimeMillis());
            HashMap<K, V> alteredEntries = new HashMap<>();
            alteredEntries.putAll(this.entries);
            buildUpdatableDbBackedCache(dbConnection, alteredEntries, false);

            synchronized (this) {
                this.entries = alteredEntries;
                this.lastLoadTimeStamp = refreshAttemptTimeStamp;
            }

        } catch (BlueWhaleCacheException e) {
            throw new BlueWhaleCacheInitializationException("Refresh failed!!", e);
        }
    }

    private void buildNonUpdatableDbBackedCache(Connection dbConnection,
                                                HashMap<K, V> entries) throws BlueWhaleCacheException {

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = dbConnection.createStatement();
            rs = stmt.executeQuery(query);
            // Code to populate cache, update entries and evict entries
            dbLoaderDelegate.addEntry(rs, entries);
        } catch (SQLException e) {
            throw new BlueWhaleCacheException("SQL Exception while querying DB. " +
                    "Init/Update failed!! for cache named : " + name, e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                log.warn("Can't close DB statement! My Cache was populated though. " +
                        "Hence I don't care and will swallow this", e);
            }
        }

        log.info("Done loading nonUpdatable cache : " + name);
    }

    /**
     * @param dbConnection: Connection to DBSource
     * @param entries:      HashMap containing (K,V) pairs for this Cache
     * @param fullReload:   If set Reload all the entries of cache, else only updated entries from
     *                      last reload
     * @throws BlueWhaleCacheException Client need to take care of providing consistent view of
     *                                 Cache with in a transaction
     */
    private void buildUpdatableDbBackedCache(Connection dbConnection,
                                             HashMap<K, V> entries,
                                             boolean fullReload) throws BlueWhaleCacheException {

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = dbConnection.prepareStatement(this.query);
            if (fullReload) {
                pstmt.setTimestamp(1, new Timestamp(1000L));
            } else {
                pstmt.setTimestamp(1, this.lastLoadTimeStamp);
            }
            rs = pstmt.executeQuery();
            // Code to populate cache, update entries and evict entries
            dbLoaderDelegate.addEntry(rs, entries);
        } catch (SQLException e) {
            throw new BlueWhaleCacheException("SQL Exception while querying DB. " +
                    "Init/Update failed!! for cache named : " + name, e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                log.warn("Can't close DB statement! My Cache was populated though. " +
                        "Hence I don't care and will swallow this", e);
            }
        }

        log.info("Done loading/refreshing Updatable cache : " + name);
    }


    public void unsafeUpdate(Connection dbConnection) throws BlueWhaleCacheUpdationException {

        HashMap<K, V> auxiliaryCache = new HashMap<>();
        Timestamp updateTimeStamp = new Timestamp(System.currentTimeMillis());

        if (StringUtils.isEmpty(query) || null == dbConnection) {
            throw new BlueWhaleCacheUpdationException("Invalid query!!");
        }
        try {
            if (isCacheUpdatable()) {
                buildUpdatableDbBackedCache(dbConnection, auxiliaryCache, true);
            } else {
                buildNonUpdatableDbBackedCache(dbConnection, auxiliaryCache);
            }
        } catch (BlueWhaleCacheException e) {
            throw new BlueWhaleCacheUpdationException("Update failed!!", e);
        }

        // Almost contention free, so not very expensive
        synchronized (this) {
            this.entries = auxiliaryCache;
            this.lastLoadTimeStamp = updateTimeStamp;
        }
    }

    @Override
    public Map<K, V> getAll() throws BlueWhaleCacheUpdationException {
        return entries;
    }

    @Override
    public int getSize() {
        return this.entries.size();
    }
}
