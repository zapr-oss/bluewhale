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

import com.aerospike.client.query.RecordSet;

import java.util.Map;

/**
 * Interface for telling how should cache be populated
 */
public interface AerospikeLoaderDelegate {
    /**
     * Closing of resultset is handled in library
     *
     * @param recordSet Entire set of keys and records
     * @param entries   Cache entries
     * @param <K>       Type of Cache's Element Key
     * @param <V>       Type of Cache Element
     */
    public <K, V> void addEntry(RecordSet recordSet, Map<K, V> entries);
}
