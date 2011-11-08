/*
x * This file is provided to you under the Apache License, Version 2.0 (the
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

import static com.basho.riak.hadoop.config.ClientFactory.getClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.hadoop.config.NoRiakLocationsException;
import com.basho.riak.hadoop.config.RiakConfig;
import com.basho.riak.hadoop.config.RiakLocation;
import com.basho.riak.hadoop.keylisters.KeyLister;

/**
 * @author russell
 * 
 */
public class RiakInputFormat extends InputFormat<BucketKey, RiakResponse> {

    private static final int MINIMUM_SPLIT = 10;

    @Override public RecordReader<BucketKey, RiakResponse> createRecordReader(InputSplit split,
                                                                              TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new RiakRecordReader();
    }

    @Override public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        RiakLocation[] locations = RiakConfig.getRiakLocatons(conf);

        if (locations.length == 0) {
            throw new NoRiakLocationsException();
        }

        final KeyLister keyLister = RiakConfig.getKeyLister(conf);

        try {
            List<BucketKey> keys = getKeys(locations, keyLister, 0);
            List<InputSplit> splits = getSplits(keys, locations,
                                                getSplitSize(keys.size(), RiakConfig.getHadoopClusterSize(conf, 3)));
            return splits;
        } catch (RiakException e) {
            throw new IOException(e);
        }
    }

    /**
     * Get the list of input keys for the task. If the first location fails, try
     * the next, and so on, until we have a success or definitive failure.
     * 
     * @return the list of bucket/keys (may be empty, never null)
     * @throws RiakException
     */
    public static List<BucketKey> getKeys(RiakLocation[] locations, KeyLister keyLister, int attemptNumber)
            throws RiakException {
        final List<BucketKey> keys = new ArrayList<BucketKey>();
        try {
            IRiakClient attemptClient = getClient(locations[attemptNumber]);
            keys.addAll(keyLister.getKeys(attemptClient));
        } catch (RiakException e) {
            if (attemptNumber >= (locations.length - 1)) {
                throw e;
            } else {
                getKeys(locations, keyLister, ++attemptNumber);
            }
        }
        return keys;
    }

    /**
     * Calculates the split size. Uses a *rough* heuristic based on the info
     * here http://wiki.apache.org/hadoop/HowManyMapsAndReduces to generate ~10
     * splits per hadoop node. Falls back to some lower number if the inputs are
     * smaller, and lower still when there are less inputs than hadoop nodes
     * 
     * @param numberOfKeys
     *            the total input size
     * @param hadoopClusterSize
     *            rough number of nodes in the hadoop m/r cluster
     * @return the size for each split
     */
    public static int getSplitSize(int numberOfKeys, int hadoopClusterSize) {
        int splitSize = numberOfKeys / (hadoopClusterSize * 10);
        if (splitSize < MINIMUM_SPLIT) {
            // too few? then use a smaller divider
            splitSize = numberOfKeys / hadoopClusterSize;
            if (splitSize < MINIMUM_SPLIT) {
                // still too few? just split into splits of MINIMUM_SPLIT
                splitSize = MINIMUM_SPLIT;
            }
        }
        return splitSize;
    }

    /**
     * Generate the splits, each split (except maybe the last) will be
     * <code>splitSize</code> and will have a {@link RiakLocation} assigned to
     * it. The {@link RiakLocation} is chosen by modulus so it should be a
     * reasonably fair distribution.
     * 
     * @param keys
     *            the list of inputs
     * @param locations
     *            all the riak locations
     * @param splitSize
     *            The target size for each split
     * @return the input splits
     */
    public static List<InputSplit> getSplits(final List<BucketKey> keys, final RiakLocation[] locations, int splitSize) {
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        int splitCnt = 0;
        int startIndex = 0;
        int numberOfKeys = keys.size();
        while (startIndex < numberOfKeys) {
            int endIndex = Math.min(numberOfKeys, splitSize + startIndex);
            final List<BucketKey> split = keys.subList(startIndex, endIndex);
            splits.add(new RiakInputSplit(split, locations[splitCnt % locations.length]));
            splitCnt++;
            startIndex = endIndex;
        }

        return splits;
    }
}