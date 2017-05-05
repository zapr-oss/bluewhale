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

package com.zapr.bluewhale.cache.file;

import com.zapr.bluewhale.cache.BlueWhaleCache;
import com.zapr.bluewhale.exception.BlueWhaleCacheException;
import com.zapr.bluewhale.exception.BlueWhaleCacheInitializationException;
import com.zapr.bluewhale.exception.BlueWhaleCacheUpdationException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileBlueWhaleCache<K, V> implements BlueWhaleCache<K, V, File> {

    @Getter
    private final boolean isCacheUpdatable;
    //refreshInterval in seconds, used only in case of updatable caches
    @Getter
    private final int refreshInterval;
    // No thread safety guarantees here, but will never be an issue
    // as its reference will always change atomically
    private HashMap<K, V> entries;
    // delegate who exposes actual implementation of adding entries in cache
    private FileReaderDelegate fileLoaderDelegate;
    @Getter
    private String name;
    @Getter
    private boolean cacheInitialized = false;
    @Getter
    private long lastLoadTimeStamp;


    public FileBlueWhaleCache(FileReaderDelegate fileReaderDelegate, String name,
                              boolean isUpdatable, int refreshInterval) {
        this.fileLoaderDelegate = fileReaderDelegate;
        this.name = name;
        this.entries = new HashMap<K, V>();
        this.isCacheUpdatable = isUpdatable;
        this.lastLoadTimeStamp = 0L;
        this.refreshInterval = refreshInterval;
    }

    public FileBlueWhaleCache(FileReaderDelegate fileReaderDelegate, String name,
                              int refreshInterval) {
        this(fileReaderDelegate, name, true, refreshInterval);
    }

    public V query(K key) {
        return entries.get(key);
    }

    public void init(File file) throws BlueWhaleCacheInitializationException {

        if (file == null || !file.exists()) {
            throw new BlueWhaleCacheInitializationException("Null or Invalid file path!!");
        }
        try {
            long initTimeStamp = System.currentTimeMillis();
            BuildFileBasedCache(file, this.entries);
            this.lastLoadTimeStamp = initTimeStamp;
        } catch (BlueWhaleCacheException e) {
            throw new BlueWhaleCacheInitializationException("Init failed!!", e);
        }
        cacheInitialized = true;

    }

    public void refresh(File file) throws BlueWhaleCacheInitializationException,
            BlueWhaleCacheUpdationException {

        if (file == null || !file.exists()) {
            throw new BlueWhaleCacheInitializationException("Null or Invalid file path!!");
        }

        if (!isCacheUpdatable()) {
            throw new BlueWhaleCacheUpdationException("Refresh failed for nonUpdatable cache");
        }

        try {
            long refreshTimeStamp = System.currentTimeMillis();
            if (file.lastModified() > this.lastLoadTimeStamp) {
                HashMap<K, V> newEntries = new HashMap<>();
                BuildFileBasedCache(file, newEntries);
                synchronized (this) {
                    this.entries = newEntries;
                }
            }
            this.lastLoadTimeStamp = refreshTimeStamp;
        } catch (BlueWhaleCacheException e) {
            throw new BlueWhaleCacheInitializationException("Refresh failed!!", e);
        }
    }

    private void BuildFileBasedCache(File file, HashMap<K, V> entries)
            throws BlueWhaleCacheException {

        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new BlueWhaleCacheException("Can't read given file");
        }

        try {
            try {
                // Code to populate cache
                fileLoaderDelegate.addEntry(br, entries);
                log.info("Done loading cache : " + name);
            } catch (IOException e) {
                throw new BlueWhaleCacheException("IOException while reading file. " +
                        "Init/Update failed!! for cache named : " + name, e);
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.warn("Can't close File Reader! My Cache was populated though. " +
                            "Hence I don't care and will swallow this", e);
                }
            }
        }
    }

    public void unsafeUpdate(File fileSource) throws BlueWhaleCacheUpdationException {

        HashMap<K, V> auxiliaryCache = new HashMap<>();
        long updateTimestamp = System.currentTimeMillis();

        if (fileSource == null || !fileSource.exists()) {
            throw new BlueWhaleCacheUpdationException("Null or Invalid file path while unsafeUpdate!!");
        }
        try {
            BuildFileBasedCache(fileSource, auxiliaryCache);
        } catch (BlueWhaleCacheException e) {
            throw new BlueWhaleCacheUpdationException("Update failed!!", e);
        }

        // Almost contention free, so not very expensive
        synchronized (this) {
            this.entries = auxiliaryCache;
            this.lastLoadTimeStamp = updateTimestamp;
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
