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

package com.zapr;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.zapr.bluewhale.cache.aerospike.AerospikeCache;
import com.zapr.bluewhale.cache.aerospike.AerospikeLoaderDelegate;
import com.zapr.bluewhale.exception.BlueWhaleCacheInitializationException;

import org.easymock.EasyMock;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@PrepareForTest({RecordSet.class, AerospikeClient.class})
public class AerospikeCacheTest {

    /**
     * We need a special {@link IObjectFactory}.
     * for running with TestNG
     *
     * @return {@link PowerMockObjectFactory}.
     */
    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testAerospikeCacheNull() {

        AerospikeCache cache = new AerospikeCache(null, null, null, null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testAerospikeCacheDelegateNull() {

        AerospikeCache cache = new AerospikeCache(null, "Aerospike Cache", new Statement(), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testAerospikeCacheNameNull() {

        AerospikeLoaderDelegate aerospikeLoaderDelegate = EasyMock.createMock(AerospikeLoaderDelegate.class);
        AerospikeCache cache = new AerospikeCache(aerospikeLoaderDelegate, null, new Statement(), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testAerospikeCacheStatementNull() {

        AerospikeLoaderDelegate aerospikeLoaderDelegate = EasyMock.createMock(AerospikeLoaderDelegate.class);
        AerospikeCache cache = new AerospikeCache(aerospikeLoaderDelegate, "Cache", null, null);
    }

    @Test
    public void testAerospikeCacheStatementConstructor() {

        String cacheName = "Aerospike Cache";
        AerospikeLoaderDelegate aerospikeLoaderDelegate = EasyMock.createMock(AerospikeLoaderDelegate.class);
        AerospikeCache cache = new AerospikeCache(aerospikeLoaderDelegate, cacheName, new Statement(), null);

        Assert.assertEquals(cache.getName(), cacheName);
    }

    @Test
    public void testAerospikeCache() throws BlueWhaleCacheInitializationException {

        String cacheName = "Aerospike Cache";
        String namespace = "namespace";
        String set = "set";
        String keyString1 = "key1";
        String keyString2 = "key2";
        String binName = "bin";

        Map<String, Object> binNameToElementMap = new HashMap<>();
        binNameToElementMap.put(binName, 1);

        TestLoaderDelegate testLoaderDelegate = new TestLoaderDelegate();
        Statement statement = new Statement();

        // Using PowerMock since AerospikeClient and RecordSet are final classes and final classes cannot be mocked
        // by EasyMock alone.
        AerospikeClient aerospikeClient = PowerMock.createPartialMock(AerospikeClient.class, "query");
        RecordSet recordSet = PowerMock.createMock(RecordSet.class);

        Key key1 = new Key(namespace, set, keyString1);
        Record record = new Record(binNameToElementMap, 1, 1);
        Key key2 = new Key(namespace, set, keyString2);

        EasyMock.expect(recordSet.next()).andReturn(true).once();
        EasyMock.expect(recordSet.getKey()).andReturn(key1).once();
        EasyMock.expect(recordSet.getRecord()).andReturn(record).once();
        EasyMock.expect(recordSet.next()).andReturn(true).once();
        EasyMock.expect(recordSet.getKey()).andReturn(key2).once();
        EasyMock.expect(recordSet.getRecord()).andReturn(record).once();
        EasyMock.expect(recordSet.next()).andReturn(false).once();
        recordSet.close();
        EasyMock.expectLastCall().times(1);
        EasyMock.expect(aerospikeClient.query(null, statement)).andReturn(recordSet);

        PowerMock.replay(recordSet, aerospikeClient);

        AerospikeCache cache = new AerospikeCache(testLoaderDelegate, cacheName, statement, null);
        Assert.assertFalse(cache.isCacheInitialized());
        cache.init(aerospikeClient);

        Assert.assertEquals(cache.getSize(), 2);

        String value = (String) cache.query(keyString1);

        Assert.assertNotNull(value);
        Assert.assertEquals(value, "1");

        value = (String) cache.query(keyString2);

        Assert.assertNotNull(value);
        Assert.assertEquals(value, "1");

        Assert.assertTrue(cache.isCacheInitialized());
    }

    private class TestLoaderDelegate implements AerospikeLoaderDelegate {

        @Override
        public <K, V> void addEntry(RecordSet recordSet, Map<K, V> entries) {
            while (recordSet.next()) {
                Key key = recordSet.getKey();
                Record record = recordSet.getRecord();

                entries.put((K) key.userKey.toString(), (V) record.getValue("bin").toString());
            }

        }
    }
}