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
package com.basho.riak.hadoop.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.basho.riak.hadoop.keylisters.BucketKeyLister;
import com.basho.riak.hadoop.keylisters.KeyLister;

/**
 * Helper class to make dealing with the hadoop {@link Configuration} object
 * easier when setting up a Riak Map/Reduce job on Hadoop
 * 
 * @author russell
 * 
 */
public final class RiakConfig {

    public static final String LOCATIONS_PROPERTY = "com.basho.riak.hadoop.mr.riak.locations";
    private static final String COMMA = ",";
    public static final String CLUSTER_SIZE_PROPERTY = "com.basho.riak.hadoop.mr.cluster.size";
    private static final String KEY_LISTER_CLASS_PROPERTY = "com.basho.riak.hadoop.mr.keylister.class";
    private static final String KEY_LISTER_INIT_STRING_PROPERTY = "com.basho.riak.hadoop.mr.keylister.init_string";
    private static final String OUTPUT_BUCKET_PROPERTY = "com.basho.riak.hadoop.mr.output.bucket";

    private RiakConfig() {}

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

    /**
     * @param conf
     *            the {@link Configuration} to query
     * @return the {@link KeyLister} the job was configured with
     * @throws RuntimeException
     *             if a {@link IllegalAccessException} or
     *             {@link InstantiationException} is thrown creating a
     *             {@link KeyLister}
     */
    public static KeyLister getKeyLister(Configuration conf) throws IOException {
        Class<? extends KeyLister> clazz = conf.getClass(KEY_LISTER_CLASS_PROPERTY, BucketKeyLister.class,
                                                         KeyLister.class);
        try {
            KeyLister lister = clazz.newInstance();
            lister.init(conf.get(KEY_LISTER_INIT_STRING_PROPERTY));
            return lister;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set the {@link KeyLister} implementation to use.
     * 
     * @param conf
     *            the {@link Configuration} to update
     * @param lister
     *            the {@link KeyLister} to use
     * @return the configuration updated with a serialized version of the lister
     *         provided
     */
    public static <T extends KeyLister> Configuration setKeyLister(Configuration conf, T lister) throws IOException {
        conf.setClass(KEY_LISTER_CLASS_PROPERTY, lister.getClass(), KeyLister.class);
        conf.setStrings(KEY_LISTER_INIT_STRING_PROPERTY, lister.getInitString());
        return conf;
    }

    /**
     * Get the configured output bucket for the job's results
     * 
     * @param conf
     *            the {@link Configuration} to query
     * @return the bucket name
     */
    public static String getOutputBucket(Configuration conf) {
        return conf.get(OUTPUT_BUCKET_PROPERTY);
    }

    /**
     * Add the output bucket for the results to the config.
     * 
     * @param conf
     *            the {@link Configuration} to update
     * @param bucket
     *            the bucket to add
     * @return the updated {@link Configuration}
     */
    public static Configuration setOutputBucket(Configuration conf, String bucket) {
        conf.set(OUTPUT_BUCKET_PROPERTY, bucket);
        return conf;
    }
}
