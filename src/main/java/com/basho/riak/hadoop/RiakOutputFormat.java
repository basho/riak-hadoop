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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.basho.riak.client.RiakException;

/**
 * Riak specific {@link OutputFormat}, just creates a {@link RiakRecordWriter}
 * 
 * @author russell
 * 
 */
public class RiakOutputFormat<V> extends OutputFormat<Text, V> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs(org.apache.
     * hadoop.mapreduce.JobContext)
     */
    @Override public void checkOutputSpecs(JobContext ctx) throws IOException, InterruptedException {}

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.OutputFormat#getOutputCommitter(org.apache
     * .hadoop.mapreduce.TaskAttemptContext)
     */
    @Override public OutputCommitter getOutputCommitter(TaskAttemptContext tac) throws IOException,
            InterruptedException {
        return new RiakOutputCommitter();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.OutputFormat#getRecordWriter(org.apache.hadoop
     * .mapreduce.TaskAttemptContext)
     */
    @Override public RecordWriter<Text, V> getRecordWriter(TaskAttemptContext tac) throws IOException,
            InterruptedException {
        try {
            return new RiakRecordWriter<V>(tac);
        } catch (RiakException e) {
            throw new IOException(e);
        }
    }
}
