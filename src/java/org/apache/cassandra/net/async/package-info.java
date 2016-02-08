/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <h1>Internode messaging</h1>
 * Here we describe the internode messaging protocol, or how two cassandra servers communicate with each other.
 * Currently, each connection is unidirectional and a request sent by one node on a given connection is responded to
 * by the peer on a different connection.
 *
 * <h2>Starting a connection</h2>
 * Each connection between two peers begins the same way:
 * - establish the TCP socket connection
 * - perform the protocol handshake
 * - send messages ... until one ends closes the socket
 *
 * Let's dig into the details of the handshake, first the protocol of messages,
 * then the details of each message. To avoid any confusion with client-server terminology,
 * we'll use "node1" to identify the host initiating a connection (the 'client'), and "node2" to identify
 * the host listening on a socket, waiting for a connection (the 'server').
 *
 * <h3>handshake protocol</h3>
 * <p>The internode messaging handshake protocol is similar to the TCP three-way handshake, in that three messages
 * are exchanged. The nodes try to agree on a protocol version and several other data points. The messages do not
 * have formal names, so we will use "message 1", "message 2", and "message 3".
 *
 * - message 1 (sent by node1): contains the protocol version node1 thinks they should use, as well as flags to indicate:
 * -- if the connection is for streaming or messaging
 * -- if messages will be compressed (applies to messaging connections only, currently)
 *
 * - message 2 (sent by node2): contains node2's protocol version.
 *
 * - message 3 (sent by node1): contains's node1's protocol version (which may be different from the protocol version
 * the nodes choose to communicate with), and it's identifying IP address (which might be different from the address
 * it is connecting from).
 *
 * <p>If the connection is for streaming, then only the first message of this protocol applies as streaming has a
 * separate protocol (not documented here).
 *
 * <h3>handshake message specification</h3>
 * <p>message 1 (sent from node1): a message of 8 bytes, consisting of a 4 byte magic value
 * ({@link org.apache.cassandra.net.MessagingService#PROTOCOL_MAGIC}, and a 4 byte heaaer. The header is defined as:
 *
 * <pre>
 * {@code
 *                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |U U C         S|                |                               |
 * |N N M         T|     VERSION    |             unused            |
 * |U U P         R|                |                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 * UNU - unused bits lowest two bits; from a historical note: used to be "serializer type," which was always Binary
 * CMP - compression enabled bit
 * STR - flag to indicate if the connection is for streaming; if not, it is for internode messaging
 * VERSION - if a streaming connection, indicates the streaming protocol version {@link org.apache.cassandra.streaming.messages.StreamMessage#CURRENT_VERSION};
 * if a messaging connection, indicates the messaging protocol version node1 *thinks* node2 is on, or if no data on node2,
 * {@link org.apache.cassandra.net.MessagingService#current_version}.
 *
 * <p>message 2 (sent from node2): a message of 4 four bytes, consisting of one (4 byte) value: node2's
 * messaging protocol version {@link org.apache.cassandra.net.MessagingService#current_version}.
 *
 * <p>message 3 (sent from node1): a meesage of 9 or 21 bytes, in which node1 sends it's
 * {@link org.apache.cassandra.net.MessagingService#current_version} (4 bytes), follwed by it's broadcast address.
 * The address is encoded as per {@link org.apache.cassandra.net.CompactEndpointSerializationHelper}, which writes out
 * 5 bytes for an IPv4 address, or 17 bytes for IPv6.
 *
 * <h2>Sending messages</h2>
 * <p>After the connection is established and handshake successful, node1 is can start sending messages at will.
 *
 * <h3>message format</h3>
 * Each message contains a header with several fixed fields, an optional key-value parameters section, and then
 * the message payload itself. Note: the IP address in the header may be either IPv4 (4 bytes) or IPv6 (16 bytes).
 * The diagram below shows the IPv4 address for brevity.
 *
 * <pre>
 * {@code
 *            1 1 1 1 1 2 2 2 2 2 3 3 3 3 3 4 4 4 4 4 5 5 5 5 5 6 6
 *  0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                       PROTOCOL MAGIC                          |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Message ID                            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Timestamp                             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Addr len |           IP Address (IPv4)                       /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /           |                 Verb                              /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /           |            Parameters size                        /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /           |             Parameter data                        /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Payload size                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               /
 * /                           Payload                             /
 * /                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 *
 * An individual parameter has a String key and a byte array value. The key is serialized with it's length,
 * encoded as two bytes, followed by the UTF-8 byte encoding of the string (see {@link java.io.DataOutput#writeUTF(java.lang.String)}).
 * The body is serialized with it's length, encoded as four bytes, followed by the bytes of the value.
 *
 * <h1>Implementation notes</h1>
 *
 * // TODO:JEB describe the non-blocking IO, netty implementation
 */
package org.apache.cassandra.net.async;