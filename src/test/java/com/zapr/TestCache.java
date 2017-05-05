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

import com.zapr.bluewhale.cache.file.FileBlueWhaleCache;
import com.zapr.bluewhale.cache.file.FileReaderDelegate;
import com.zapr.bluewhale.cache.sqlDB.DBBlueWhaleCache;
import com.zapr.bluewhale.cache.sqlDB.DBLoaderDelegate;
import com.zapr.bluewhale.exception.BlueWhaleCacheInitializationException;
import com.zapr.bluewhale.exception.BlueWhaleCacheUpdationException;

import org.apache.commons.lang.StringUtils;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCache {

    @Test
    public void testInstantiation() throws SQLException {

        Connection dbConnection = EasyMock.createMock(Connection.class);
        String query = "testQuery";

        DBLoaderDelegate dbLoaderDelegate = EasyMock.createMock(DBLoaderDelegate.class);
        DBBlueWhaleCache cache = new DBBlueWhaleCache(dbLoaderDelegate, "testDBCache", query,
                true, 60);
        AssertJUnit.assertNotNull(cache);
        AssertJUnit.assertEquals("testDBCache", cache.getName());

        Statement stmt = EasyMock.createMock(Statement.class);
        EasyMock.expect(dbConnection.createStatement()).andReturn(stmt).anyTimes();

        ResultSet rs = EasyMock.createMock(ResultSet.class);
        EasyMock.expect(stmt.executeQuery(query)).andReturn(rs).anyTimes();
    }

    @Test
    void testCacheInit() {
        FileReaderDelegate readerDelegate = new CandidateReaderDelegate();
        FileBlueWhaleCache fileCache = new FileBlueWhaleCache(readerDelegate,
                "election_candidates", 60);
        try {
            fileCache.init(new File("src/test/resources/data.csv"));
        } catch (BlueWhaleCacheInitializationException e) {
            Assert.fail("Failure to init cache");
        }

        try {
            Assert.assertEquals(fileCache.getAll().size(), 3);
        } catch (BlueWhaleCacheUpdationException e) {
            Assert.fail("Failure to get complete collection");
        }
        Candidate candidate = (Candidate) fileCache.query("1");
        Assert.assertNotNull(candidate);
        Assert.assertEquals(candidate.getName(), "Sunita");
        Assert.assertEquals(candidate.getId(), 1);
        Assert.assertEquals(candidate.getAssets(), 90);
        Assert.assertEquals(candidate.getQualification(), "BA");
        Assert.assertEquals(candidate.getProb(), 0.46f);
    }

    private abstract class CSVReaderDelegate<K, V> implements FileReaderDelegate {
        @Override
        public <K, V> void addEntry(BufferedReader br, HashMap<K, V> entries) {

            String row = "";
            try {
                while ((row = br.readLine()) != null) {
                    String[] vals = StringUtils.split(row, ",");

                    // Ignore if it is header
                    if (vals[0].contains("id")) {
                        continue;
                    }
                    // assumption here is first value is always integer id

                    entries.put((K) vals[0], (V) getObjectFromValues(vals));
                }
            } catch (IOException e) {
                log.warn("Exception in reading line", e);
            }

        }

        protected abstract V getObjectFromValues(String[] vals);
    }

    private class CandidateReaderDelegate extends CSVReaderDelegate<Integer, Candidate> {

        @Override
        protected Candidate getObjectFromValues(String[] vals) {
            return new Candidate(Integer.valueOf(StringUtils.trim(vals[0])), vals[1],
                    vals[2], Integer.valueOf(StringUtils.trim((vals[3]))), Float.valueOf(StringUtils.trim(vals[4])));
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    private class Candidate {
        private int id;
        private String name;
        private String qualification;
        private int assets;
        private float prob;
    }
}
