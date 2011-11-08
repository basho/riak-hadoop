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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.hadoop.config.ClientFactory;
import com.basho.riak.hadoop.config.RiakConfig;

/**
 * @author russell
 * @param <V>
 * 
 */
public class RiakRecordWriter<V> extends RecordWriter<Text, V> {

    private final Bucket bucket;

    RiakRecordWriter(TaskAttemptContext tac) throws RiakException {
        Configuration conf = tac.getConfiguration();
        IRiakClient client = ClientFactory.clusterClient(RiakConfig.getRiakLocatons(conf));
        bucket = client.fetchBucket(RiakConfig.getOutputBucket(conf)).execute();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce
     * .TaskAttemptContext)
     */
    @Override public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
        // NO-OP
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object,
     * java.lang.Object)
     */
    @Override public void write(Text key, V value) throws IOException, InterruptedException {
        try {
            bucket.store(key.toString(), value).execute();
        } catch (RiakException e) {
            throw new IOException(e);
        }
    }
}
