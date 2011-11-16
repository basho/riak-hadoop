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

import java.net.URI;

/**
 * Models a Riak API end point location
 * 
 * @author russell
 * 
 */
public abstract class RiakLocation {

    private final RiakTransport transport;
    private final String host;
    private final int port;

    /**
     * Create a location
     * 
     * @param transport
     *            the {@link RiakTransport} for this location
     * @param host
     *            the host
     * @param port
     *            the port
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

    /**
     * Serialize this location to a String
     * 
     * @return a string representation that can be used by fromString(String)
     */
    public abstract String asString();

    /**
     * De-serialize the location from a String
     * 
     * @param location
     *            a String representation from asString()
     * @return a {@link RiakLocation}
     */
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

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        result = prime * result + ((transport == null) ? 0 : transport.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RiakLocation)) {
            return false;
        }
        RiakLocation other = (RiakLocation) obj;
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        if (transport != other.transport) {
            return false;
        }
        return true;
    }
}
