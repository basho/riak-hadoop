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

import com.basho.riak.hadoop.RiakInputFormat;

/**
 * Tag exception for hadoop config where no {@link RiakLocation}s have been
 * provided to the {@link RiakInputFormat}
 * 
 * @author russell
 * 
 */
public class NoRiakLocationsException extends IOException {

    /**
     * Eclipse generated
     */
    private static final long serialVersionUID = -4095183778220854984L;

}
