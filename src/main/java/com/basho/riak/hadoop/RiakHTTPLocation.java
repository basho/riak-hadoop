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

/**
 * @author russell
 * 
 */
public class RiakHTTPLocation extends RiakLocation {

    private final String riakPath;

    /**
     * @param transport
     * @param host
     * @param port
     * @param riakPath
     */
    protected RiakHTTPLocation(String host, int port, String riakPath) {
        super(RiakTransport.HTTP, host, port);
        this.riakPath = riakPath;
    }

    /**
     * @return the riakPath
     */
    public synchronized String getRiakPath() {
        return riakPath;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.RiakLocation#asString()
     */
    @Override public String asString() {
        StringBuilder sb = new StringBuilder("http://");
        sb.append(getHost()).append(":").append(getPort());

        if (!riakPath.startsWith("/")) {
            sb.append("/");
        }

        sb.append(riakPath);
        return sb.toString();
    }
}
