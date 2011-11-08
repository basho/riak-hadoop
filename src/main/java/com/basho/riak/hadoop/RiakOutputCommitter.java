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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A NO-OP output committer
 * 
 * @author russell
 *
 */
public class RiakOutputCommitter extends OutputCommitter {

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputCommitter#abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override public void abortTask(TaskAttemptContext tac) throws IOException {}

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputCommitter#commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override public void commitTask(TaskAttemptContext tac) throws IOException {}

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputCommitter#needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputCommitter#setupJob(org.apache.hadoop.mapreduce.JobContext)
     */
    @Override public void setupJob(JobContext jc) throws IOException {}

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputCommitter#setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override public void setupTask(TaskAttemptContext tac) throws IOException {}

}
