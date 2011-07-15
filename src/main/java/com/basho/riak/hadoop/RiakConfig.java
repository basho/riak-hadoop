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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Helper class to make dealing with the hadoop {@link Configuration} object
 * easier when setting up a Riak Map/Reduce job on Hadoop
 * 
 * @author russell
 * 
 */
public final class RiakConfig {

    public static final String BUCKET_PROPERTY = "com.basho.riak.hadoop.mr.bucket.name";
    public static final String LOCATIONS_PROPERTY = "com.basho.riak.hadoop.mr.riak.locations";
    private static final String COMMA = ",";
    public static final String CLUSTER_SIZE_PROPERTY = "com.basho.riak.hadoop.mr.cluster.size";

    private RiakConfig() {}

    /**
     * Retrieve the name of the bucket to map reduce over from the hadoop
     * Configuration.
     * 
     * @param conf
     *            the hadoop {@link Configuration}
     * @return the name of the bucket to use as input to the m/r
     * @throws RuntimeException
     *             if the bucket property is not set
     */
    public static String getBucket(Configuration conf) {
        String bucket = conf.get(BUCKET_PROPERTY);
        if (bucket == null) {
            throw new RuntimeException("bucket property was null");
        }
        return bucket;
    }

    /**
     * Set the bucket name to map reduce over in the provided hadoop
     * {@link Configuration}
     * 
     * @param conf
     *            the hadoop {@link Configuration} to update
     * @param bucketName
     *            the name of the bucket to use as input to a hadoop m/r job
     * @return the {@link Configuration} updated with the bucketName property
     *         set to <code>bucketName</code>
     */
    public static Configuration setBucket(Configuration conf, String bucketName) {
        conf.set(BUCKET_PROPERTY, bucketName);
        return conf;
    }

    /**
     * Add a riak location to the {@link Configuration} passed.
     * 
     * @param conf
     *            the {@link Configuration} to add a location too
     * @param location
     *            the {@link RiakLocation} to add
     * @return the {@link Configuration} with <code>location</code> added to the
     *         location property
     */
    public static Configuration addLocation(Configuration conf, RiakLocation location) {
        StringBuilder sb = new StringBuilder();
        String currentLocations = conf.get(LOCATIONS_PROPERTY);

        if (currentLocations != null) {
            sb.append(currentLocations);
        }

        if (sb.length() > 0) {
            sb.append(COMMA);
        }

        sb.append(location.asString());
        
        conf.set(LOCATIONS_PROPERTY, sb.toString());
        return conf;
    }

    /**
     * Get all the riak locations from the passed {@link Configuration}
     * 
     * @param conf
     *            the {@link Configuration}
     * @return an array of {@link RiakLocation} (may be empty, never null)
     */
    public static RiakLocation[] getRiakLocatons(Configuration conf) {
        String locations = conf.get(LOCATIONS_PROPERTY, "");
        StringTokenizer st = new StringTokenizer(locations, COMMA);
        List<RiakLocation> result = new ArrayList<RiakLocation>();

        while (st.hasMoreTokens()) {
            result.add(RiakLocation.fromString(st.nextToken()));
        }

        return result.toArray(new RiakLocation[result.size()]);
    }

    /**
     * Set the size of the hadoop cluster, this is used by the
     * {@link RiakInputFormat} to try and optimize the number of
     * {@link InputSplit}s to create
     * 
     * @param conf
     *            the {@link Configuration} to store the hadoop cluster size in
     * @param hadoopClusterSize
     *            the size of the hadoop cluster
     * @return the {@link Configuration} updated with the passed
     *         <code>hadoopClusterSize</code>
     */
    public static Configuration setHadoopClusterSize(Configuration conf, int hadoopClusterSize) {
        conf.setInt(CLUSTER_SIZE_PROPERTY, hadoopClusterSize);
        return conf;

    }

    /**
     * Get the hadoop cluster size property, provide a default in case it hasn't
     * been set
     * 
     * @param conf
     *            the {@link Configuration} to get the property value from
     * @param defaultValue
     *            the default size to use if it hasn't been set
     * @return the hadoop cluster size or <code>defaultValue</code>
     */
    public static int getHadoopClusterSize(Configuration conf, int defaultValue) {
        return conf.getInt(CLUSTER_SIZE_PROPERTY, defaultValue);
    }
}
