/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.hadoop.config.NoRiakLocationsException;
import com.basho.riak.hadoop.config.RiakLocation;
import com.basho.riak.hadoop.config.RiakPBLocation;

/**
 * @author russell
 * 
 */
public class RiakInputFormatTest {

    private static final String BUCKET = "bucket";
    private static final String KEY = "key";

    @Mock public JobContext jobContext;

    private RiakInputFormat inputFormat;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        inputFormat = new RiakInputFormat();
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.RiakInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)}
     * .
     */
    @Test public void getSplits_noLocations() throws Exception {
        Configuration conf = new Configuration();
        when(jobContext.getConfiguration()).thenReturn(conf);
        try {
            inputFormat.getSplits(jobContext);
            fail("Expected IOException");
        } catch (NoRiakLocationsException e) {
            // NO-OP
        }
    }

    @Test public void getSplitSize() {
        assertEquals(10, RiakInputFormat.getSplitSize(10, 4));
        assertEquals(20, RiakInputFormat.getSplitSize(800, 4));
        assertEquals(2500, RiakInputFormat.getSplitSize(100000, 4));
    }

    @Test public void getSplits() throws Exception {
        final List<BucketKey> bks = new LinkedList<BucketKey>();
        for (int i = 0; i < 100001; i++) {
            bks.add(new BucketKey(BUCKET, KEY + i));
        }

        RiakLocation[] locations = new RiakLocation[] { new RiakPBLocation("host1", 8091),
                                                       new RiakPBLocation("host2", 8091),
                                                       new RiakPBLocation("host3", 8091),
                                                       new RiakPBLocation("host4", 8091) };

        List<InputSplit> splits = RiakInputFormat.getSplits(bks, locations, 999);

        assertEquals("Expected 101 splits", 101, splits.size());

        int _999SplitCnt = 0;
        int _101SplitCnt = 0;
        int otherSplitCnt = 0;

        for (InputSplit is : splits) {
            long length = is.getLength();

            if (length == 999) {
                _999SplitCnt++;
            } else if (length == 101) {
                _101SplitCnt++;
            } else {
                otherSplitCnt++;
            }
        }

        assertEquals("Should be 100 splits of 999 keys", 100, _999SplitCnt);
        assertEquals("Should be 1 split of 101 keys", 1, _101SplitCnt);
        assertEquals("Should be 0 splits of with neither 999 or 101 keys", 0, otherSplitCnt);
    }
}
