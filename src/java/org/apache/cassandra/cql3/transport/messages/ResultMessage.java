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
package org.apache.cassandra.cql3.transport.messages;

import java.nio.ByteBuffer;
import java.util.*;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.transport.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public abstract class ResultMessage extends Message.Response
{
    private final static String COUNT_COLUMN = "count";

    public static final Message.Codec<ResultMessage> codec = new Message.Codec<ResultMessage>()
    {
        public ResultMessage decode(ChannelBuffer body)
        {
            Kind kind = Kind.fromId(body.readInt());
            return kind.subcodec.decode(body);
        }

        public ChannelBuffer encode(ResultMessage msg)
        {
            ChannelBuffer kcb = ChannelBuffers.buffer(4);
            kcb.writeInt(msg.kind.id);

            ChannelBuffer body = msg.encodeBody();
            return ChannelBuffers.wrappedBuffer(kcb, body);
        }
    };

    private enum Kind
    {
        VOID         (0, Void.subcodec),
        ROWS         (1, Rows.subcodec),
        SET_KEYSPACE (2, SetKeyspace.subcodec),
        PREPARED     (3, Prepared.subcodec);

        public final int id;
        public final Message.Codec<ResultMessage> subcodec;

        private static final Kind[] ids;
        static
        {
            int maxId = -1;
            for (Kind k : Kind.values())
                maxId = Math.max(maxId, k.id);
            ids = new Kind[maxId + 1];
            for (Kind k : Kind.values())
            {
                if (ids[k.id] != null)
                    throw new IllegalStateException("Duplicate kind id");
                ids[k.id] = k;
            }
        }

        private Kind(int id, Message.Codec<ResultMessage> subcodec)
        {
            this.id = id;
            this.subcodec = subcodec;
        }

        public static Kind fromId(int id)
        {
            Kind k = ids[id];
            if (k == null)
                throw new ProtocolException(String.format("Unknown kind id %d in RESULT message", id));
            return k;
        }
    }

    private final Kind kind;

    protected ResultMessage(Kind kind)
    {
        super(Message.Type.RESULT);
        this.kind = kind;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    protected abstract ChannelBuffer encodeBody();

    public abstract CqlResult toThriftResult();

    public static class Void extends ResultMessage
    {
        // use VOID_MESSAGE
        private Void()
        {
            super(Kind.VOID);
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                return Void.instance();
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof Void;
                return ChannelBuffers.EMPTY_BUFFER;
            }
        };

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            return new CqlResult(CqlResultType.VOID);
        }

        public static Void instance()
        {
            return Holder.instance;
        }

        // Battling java initialization
        private static class Holder
        {
            static final Void instance = new Void();
        }

        @Override
        public String toString()
        {
            return "EMPTY RESULT";
        }
    }

    public static class SetKeyspace extends ResultMessage
    {
        private final String keyspace;

        public SetKeyspace(String keyspace)
        {
            super(Kind.SET_KEYSPACE);
            this.keyspace = keyspace;
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                String keyspace = CBUtil.readString(body);
                return new SetKeyspace(keyspace);
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof SetKeyspace;
                return CBUtil.stringToCB(((SetKeyspace)msg).keyspace);
            }
        };

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            return new CqlResult(CqlResultType.VOID);
        }

        @Override
        public String toString()
        {
            return "RESULT set keyspace " + keyspace;
        }
    }

    public static class Rows extends ResultMessage
    {
        /*
         * Format:
         *   - header (12 bytes)
         *     - 4 bytes: flags
         *     - 4 bytes: rows count
         *     - 4 bytes: columns count
         *   - metadata (depends on flags)
         *   - rows
         */
        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                // header
                int flags = body.readInt();
                int rowCount = body.readInt();
                int columnCount = body.readInt();

                // metadata
                List<Pair<String, String>> metadata = new ArrayList<Pair<String, String>>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    String name = CBUtil.readString(body);
                    String type = CBUtil.readString(body);
                    metadata.add(Pair.create(name, type));
                }

                Rows rows = new Rows(Flag.deserialize(flags), metadata);

                // rows
                int totalValues = rowCount * columnCount;
                for (int i = 0; i < totalValues; i++)
                    rows.addColumnValue(CBUtil.readValue(body));
                return rows;
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof Rows;

                Rows rowMsg = (Rows)msg;
                ChannelBuffer header = ChannelBuffers.buffer(12);
                header.writeInt(Flag.serialize(rowMsg.flags));
                header.writeInt(rowMsg.rows.size());
                header.writeInt(rowMsg.metadata.size());

                // We have:
                //   - the header
                //   - 2 * metadata strings
                //   - metatada * rows values
                int ms = rowMsg.metadata.size();
                int vs = ms * rowMsg.rows.size();
                CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(1, 2 * ms, vs);
                builder.add(header);

                for (Pair<String, String> p : rowMsg.metadata)
                    builder.addString(p.left).addString(p.right);

                for (List<ByteBuffer> row : rowMsg.rows)
                {
                    for (ByteBuffer bb : row)
                        builder.addValue(bb);
                }

                return builder.build();
            }
        };

        private final EnumSet<Flag> flags;
        public final List<Pair<String, String>> metadata;
        public final List<List<ByteBuffer>> rows = new ArrayList<List<ByteBuffer>>();

        public Rows(List<Pair<String, String>> metadata)
        {
            this(EnumSet.of(Flag.FIRST_FRAME, Flag.LAST_FRAME), metadata);
        }

        private Rows(EnumSet<Flag> flags, List<Pair<String, String>> metadata)
        {
            super(Kind.ROWS);
            this.flags = flags;
            this.metadata = new ArrayList<Pair<String, String>>(metadata);
        }

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public void addColumnValue(ByteBuffer value)
        {
            if (rows.isEmpty() || lastRow().size() == metadata.size())
                rows.add(new ArrayList<ByteBuffer>(metadata.size()));

            lastRow().add(value);
        }

        private List<ByteBuffer> lastRow()
        {
            return rows.get(rows.size() - 1);
        }

        public void reverse()
        {
            Collections.reverse(rows);
        }

        public void trim(int limit)
        {
            int toRemove = rows.size() - limit;
            if (toRemove > 0)
            {
                for (int i = 0; i < toRemove; i++)
                    rows.remove(rows.size() - 1);
            }
        }

        public Rows makeCountResult()
        {
            long count = rows.size();
            metadata.clear();
            metadata.add(Pair.create(COUNT_COLUMN, "LongType"));

            rows.clear();
            rows.add(Collections.singletonList(ByteBufferUtil.bytes(count)));
            return this;
        }

        public CqlResult toThriftResult()
        {
            String UTF8 = "UTF8Type";
            CqlMetadata schema = new CqlMetadata(new HashMap<ByteBuffer, String>(),
                                                 new HashMap<ByteBuffer, String>(),
                                                 // The 2 following ones shouldn't be needed in CQL3
                                                 UTF8, UTF8);

            for (Pair<String, String> p : metadata)
            {
                schema.name_types.put(ByteBufferUtil.bytes(p.left), UTF8);
                schema.value_types.put(ByteBufferUtil.bytes(p.left), p.right);
            }

            List<CqlRow> cqlRows = new ArrayList<CqlRow>(rows.size());
            for (List<ByteBuffer> row : rows)
            {
                List<Column> thriftCols = new ArrayList<Column>(metadata.size());
                for (int i = 0; i < metadata.size(); i++)
                {
                    Column col = new Column(ByteBufferUtil.bytes(metadata.get(i).left));
                    col.setValue(row.get(i));
                    thriftCols.add(col);
                }
                // The key of CqlRow shoudn't be needed in CQL3
                cqlRows.add(new CqlRow(ByteBufferUtil.EMPTY_BYTE_BUFFER, thriftCols));
            }
            CqlResult res = new CqlResult(CqlResultType.ROWS);
            res.setRows(cqlRows).setSchema(schema);
            return res;
        }

        @Override
        public String toString()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.append("ROWS ").append(flags).append(" ");
                sb.append(metadata).append('\n');
                List<AbstractType> types = new ArrayList<AbstractType>(metadata.size());
                for (Pair<String, String> p : metadata)
                    types.add(TypeParser.parse(p.right));

                for (List<ByteBuffer> row : rows)
                {
                    for (int i = 0; i < row.size(); i++)
                    {
                        ByteBuffer v = row.get(i);
                        if (v == null)
                            sb.append(" | null");
                        else
                            sb.append(" | ").append(types.get(i).getString(v));
                    }
                    sb.append('\n');
                }
                sb.append("---");
                return sb.toString();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public static enum Flag
        {
            // The order of that enum matters!!
            FIRST_FRAME,
            LAST_FRAME;

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                Flag[] values = Flag.values();
                for (int n = 0; n < 32; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(values[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }
    }

    public static class Prepared extends ResultMessage
    {
        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ChannelBuffer body)
            {
                int id = body.readInt();
                List<String> types = CBUtil.readStringList(body);
                return new Prepared(id, types);
            }

            public ChannelBuffer encode(ResultMessage msg)
            {
                assert msg instanceof Prepared;
                Prepared prepared = (Prepared)msg;

                ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
                cb.writeInt(prepared.statementId);
                CBUtil.writeStringList(cb, prepared.types);
                return cb;
            }
        };

        public final int statementId;
        public final List<String> types;

        public Prepared(int statementId, List<String> types)
        {
            super(Kind.PREPARED);
            this.statementId = statementId;
            this.types = types;
        }

        protected ChannelBuffer encodeBody()
        {
            return subcodec.encode(this);
        }

        public CqlResult toThriftResult()
        {
            throw new UnsupportedOperationException();
        }

        public CqlPreparedResult toThriftPreparedResult()
        {
            return new CqlPreparedResult(statementId, types.size()).setVariable_types(types);
        }

        @Override
        public String toString()
        {
            return "RESULT PREPARED " + statementId + " " + types;
        }
    }
}
