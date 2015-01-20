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

public class SetSerializer<T> extends CollectionSerializer<Set<T>>
{
    // interning instances
    private static final Map<TypeSerializer<?>, SetSerializer> instances = new HashMap<TypeSerializer<?>, SetSerializer>();

    public final TypeSerializer<T> elements;

    public static synchronized <T> SetSerializer<T> getInstance(TypeSerializer<T> elements)
    {
        SetSerializer<T> t = instances.get(elements);
        if (t == null)
        {
            t = new SetSerializer<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private SetSerializer(TypeSerializer<T> elements)
    {
        this.elements = elements;
    }

    public List<ByteBuffer> serializeValues(Set<T> values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.size());
        for (T value : values)
            buffers.add(elements.serialize(value));
        return buffers;
    }

    public int getElementCount(Set<T> value)
    {
        return value.size();
    }

    public void validate(ByteBuffer bytes, Format format)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, format);
            for (int i = 0; i < n; i++)
                elements.validate(readValue(input, format));
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after set value");
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a set");
        }
    }

    public Set<T> deserialize(ByteBuffer bytes, Format format)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, format);
            Set<T> l = new LinkedHashSet<T>(n);
            for (int i = 0; i < n; i++)
            {
                ByteBuffer databb = readValue(input, format);
                elements.validate(databb);
                l.add(elements.deserialize(databb));
            }
            if (input.hasRemaining())
                throw new MarshalException("Unexpected extraneous bytes after set value");
            return l;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    /**
     * Deserializes a serialized set and returns a set of unserialized (ByteBuffer) elements.
     */
    public Set<ByteBuffer> deserializeToByteBufferCollection(ByteBuffer bytes, Format format)
    {
        ByteBuffer input = bytes.duplicate();
        int n = readCollectionSize(input, format);
        Set<ByteBuffer> s = new LinkedHashSet<>(n);
        for (int i = 0; i < n; i++)
            s.add(readValue(input, format));
        return s;
    }

    public String toString(Set<T> value)
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (T element : value)
        {
            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                sb.append("; ");
            }
            sb.append(elements.toString(element));
        }
        return sb.toString();
    }

    public Class<Set<T>> getType()
    {
        return (Class) Set.class;
    }
}
