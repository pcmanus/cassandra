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

package org.apache.cassandra.serializers;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class MapSerializer<K, V> extends CollectionSerializer<Map<K, V>>
{
    // interning instances
    private static final Map<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer> instances = new HashMap<Pair<TypeSerializer<?>, TypeSerializer<?>>, MapSerializer>();

    public final TypeSerializer<K> keys;
    public final TypeSerializer<V> values;
    private final Comparator<Pair<ByteBuffer, ByteBuffer>> comparator;

    public static synchronized <K, V> MapSerializer<K, V> getInstance(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> comparator)
    {
        Pair<TypeSerializer<?>, TypeSerializer<?>> p = Pair.<TypeSerializer<?>, TypeSerializer<?>>create(keys, values);
        MapSerializer<K, V> t = instances.get(p);
        if (t == null)
        {
            t = new MapSerializer<K, V>(keys, values, comparator);
            instances.put(p, t);
        }
        return t;
    }

    private MapSerializer(TypeSerializer<K> keys, TypeSerializer<V> values, Comparator<ByteBuffer> comparator)
    {
        this.keys = keys;
        this.values = values;
        this.comparator = (p1, p2) -> comparator.compare(p1.left, p2.left);
    }

    public List<ByteBuffer> serializeValues(Map<K, V> map)
    {
        List<Pair<ByteBuffer, ByteBuffer>> pairs = new ArrayList<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet())
            pairs.add(Pair.create(keys.serialize(entry.getKey()), values.serialize(entry.getValue())));
        Collections.sort(pairs, comparator);
        List<ByteBuffer> buffers = new ArrayList<>(pairs.size() * 2);
        for (Pair<ByteBuffer, ByteBuffer> p : pairs)
        {
            buffers.add(p.left);
            buffers.add(p.right);
        }
        return buffers;
    }

    public int getElementCount(Map<K, V> value)
    {
        return value.size();
    }

    public void validateForNativeProtocol(ByteBuffer bytes, int version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);
            for (int i = 0; i < n; i++)
            {
                keys.validate(readValue(input, version));
                values.validate(readValue(input, version));
            }
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after map value");
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    public Map<K, V> deserializeForNativeProtocol(ByteBuffer bytes, int version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);
            Map<K, V> m = new LinkedHashMap<K, V>(n);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, version);
                keys.validate(kbb);

                ByteBuffer vbb = readValue(input, version);
                values.validate(vbb);

                m.put(keys.deserialize(kbb), values.deserialize(vbb));
            }
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after map value");
            return m;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    /**
     * Extract an element from a serialized collection.
     *
     * @param collection the serialized collection. This cannot be {@code null}.
     * @param key the key to extract (This cannot be {@code null} nor {@code ByteBufferUtil.UNSET_BYTE_BUFFER}).
     * @param comparator the type to use to compare the {@code key} value to those
     * in the collection.
     * @return the value associated with {@code key} if one exists, {@code null} otherwise
     */
    public ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator)
    {
        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, Server.VERSION_3);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer kbb = readValue(input, Server.VERSION_3);
                int comparison = comparator.compareForCQL(kbb, key);
                if (comparison == 0)
                    return readValue(input, Server.VERSION_3);
                else if (comparison > 0)
                    // since the map is in sorted order, we know we've gone too far and the element doesn't exist
                    return null;
                else // comparison < 0
                    skipValue(input, Server.VERSION_3);
            }
            return null;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    /**
     * Returns the slice of a collection directly from its serialized value.
     *
     * @param collection the serialized collection. This cannot be {@code null}.
     * @param from the left bound of the slice to extract. This cannot be {@code null} but if this is
     * {@code ByteBufferUtil.UNSET_BYTE_BUFFER}, then the returned slice starts at the beginning
     * of {@code collection}.
     * @param from the right bound of the slice to extract. This cannot be {@code null} but if this is
     * {@code ByteBufferUtil.UNSET_BYTE_BUFFER}, then the returned slice stops at the end
     * of {@code collection}.
     * @param comparator the type to use to compare the {@code from} and {@code to} values to those
     * in the collection.
     * @return a valid serialized collection (possibly empty) corresponding to slice {@code [from, to]}
     * of {@code collection}.
     */
    public ByteBuffer getSliceFromSerialized(ByteBuffer collection, ByteBuffer from, ByteBuffer to, AbstractType<?> comparator)
    {
        if (from == ByteBufferUtil.UNSET_BYTE_BUFFER && to == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return collection;

        try
        {
            ByteBuffer input = collection.duplicate();
            int n = readCollectionSize(input, Server.VERSION_3);
            int startPos = input.position();
            int count = 0;
            boolean inSlice = from == ByteBufferUtil.UNSET_BYTE_BUFFER;

            for (int i = 0; i < n; i++)
            {
                int pos = input.position();
                ByteBuffer kbb = readValue(input, Server.VERSION_3); // key

                // If we haven't passed the start already, check if we have now
                if (!inSlice)
                {
                    int comparison = comparator.compareForCQL(from, kbb);
                    if (comparison <= 0)
                    {
                        // We're now within the slice
                        inSlice = true;
                        startPos = pos;
                    }
                    else
                    {
                        // We're before the slice so we know we don't care about this element
                        skipValue(input, Server.VERSION_3); // value
                        continue;
                    }
                }

                // Now check if we're done
                int comparison = to == ByteBufferUtil.UNSET_BYTE_BUFFER ? -1 : comparator.compareForCQL(kbb, to);
                if (comparison > 0)
                {
                    // We're done and shouldn't include the key we just read
                    input.position(pos);
                    break;
                }

                // Otherwise, we'll include that element
                skipValue(input, Server.VERSION_3); // value
                ++count;

                // But if we know if was the last of the slice, we break early
                if (comparison == 0)
                    break;
            }
            return copyAsNewCollection(collection, count, startPos, input.position(), Server.VERSION_3);
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a map");
        }
    }

    public String toString(Map<K, V> value)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean isFirst = true;
        for (Map.Entry<K, V> element : value.entrySet())
        {
            if (isFirst)
                isFirst = false;
            else
                sb.append(", ");
            sb.append(keys.toString(element.getKey()));
            sb.append(": ");
            sb.append(values.toString(element.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    public Class<Map<K, V>> getType()
    {
        return (Class)Map.class;
    }
}
