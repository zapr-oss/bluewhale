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

package com.zapr.bluewhale.cache;

import com.zapr.bluewhale.exception.BlueWhaleCacheInitializationException;
import com.zapr.bluewhale.exception.BlueWhaleCacheUpdationException;

import java.util.Map;

/**
 * In memory Cache which acts as a data container for all serving systems.
 * It exposes unsafeUpdate function as well, which should be used only for those caches,
 * where updates are almost non-existent or very rare (such as handset, geo etc), otherwise
 * it may lead to inconsistency within the same request.
 * For frequently updating caches it is the responsibility of the client to atomically
 * replace cache instance, which is provided in the request context.
 * <p>
 *
 * Created by siddharth on 8/12/15.
 */

public interface BlueWhaleCache<K, V, S> {

    public V query(K key);

    public void init(S source) throws BlueWhaleCacheInitializationException;

    public void unsafeUpdate(S source) throws BlueWhaleCacheUpdationException;

    public Map<K, V> getAll() throws BlueWhaleCacheUpdationException;

    public int getSize();
}
