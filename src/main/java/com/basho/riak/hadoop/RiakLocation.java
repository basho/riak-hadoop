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

import java.net.URI;

/**
 * @author russell
 * 
 */
public abstract class RiakLocation {

    private final RiakTransport transport;
    private final String host;
    private final int port;

    /**
     * @param transport
     * @param host
     * @param port
     */
    protected RiakLocation(RiakTransport transport, String host, int port) {
        this.transport = transport;
        this.host = host;
        this.port = port;
    }

    /**
     * @return the transport
     */
    public RiakTransport getTransport() {
        return transport;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    public abstract String asString();

    public static RiakLocation fromString(String location) {
        RiakLocation result = null;
        if (location.contains("/")) {
            result = parseHttpLocation(location);
        } else {
            String[] pbLoc = location.split(":");
            if (pbLoc.length != 2) {
                throw new IllegalArgumentException("Invalid locaton " + location);
            }
            result = new RiakPBLocation(pbLoc[0], Integer.parseInt(pbLoc[1]));
        }
        return result;
    }

    /**
     * @param location
     * @return
     */
    private static RiakLocation parseHttpLocation(String location) {
        final URI uri = URI.create(location);
        return new RiakHTTPLocation(uri.getHost(), uri.getPort(), uri.getPath());
    }

}
