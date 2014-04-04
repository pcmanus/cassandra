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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * A not truly-composite CType.
 */
public class SimpleCType extends AbstractCType
{
    protected final AbstractType<?> type;

    public SimpleCType(AbstractType<?> type)
    {
        this.type = type;
    }

    public boolean isCompound()
    {
        return false;
    }

    public int size()
    {
        return 1;
    }

    public int compare(Composite c1, Composite c2)
    {
        assert !c1.isStatic() && !c2.isStatic();
        if (c1.isEmpty() != c2.isEmpty())
            return c1.isEmpty() ? c1.eoc().prefixComparisonResult() : -c2.eoc().prefixComparisonResult();
        return type.compare(c1.get(0), c2.get(0));
    }

    public AbstractType<?> subtype(int i)
    {
        if (i != 0)
            throw new IndexOutOfBoundsException();
        return type;
    }

    public Composite fromByteBuffer(ByteBuffer bytes)
    {
        return !bytes.hasRemaining() ? Composites.EMPTY : new SimpleComposite(bytes);
    }

    public CBuilder builder()
    {
        return new SimpleCBuilder(this);
    }

    public CType setSubtype(int position, AbstractType<?> newType)
    {
        if (position != 0)
            throw new IndexOutOfBoundsException();
        return new SimpleCType(newType);
    }

    // Use sparingly, it defeats the purpose
    public AbstractType<?> asAbstractType()
    {
        return type;
    }

    public static class SimpleCBuilder implements CBuilder
    {
        private final CType type;
        private ByteBuffer value;

        public SimpleCBuilder(CType type)
        {
            this.type = type;
        }

        public int remainingCount()
        {
            return value == null ? 1 : 0;
        }

        public CBuilder add(ByteBuffer value)
        {
            if (this.value != null)
                throw new IllegalStateException();
            this.value = value;
            return this;
        }

        public CBuilder add(Object value)
        {
            return add(((AbstractType)type.subtype(0)).decompose(value));
        }

        public Composite build()
        {
            if (value == null || !value.hasRemaining())
                return Composites.EMPTY;

            // If we're building a dense cell name, then we can directly allocate the
            // CellName object as it's complete.
            if (type instanceof CellNameType && ((CellNameType)type).isDense())
                return new SimpleDenseCellName(value);

            return new SimpleComposite(value);
        }

        public Composite buildWith(ByteBuffer value)
        {
            if (this.value != null)
                throw new IllegalStateException();

            if (value == null || !value.hasRemaining())
                return Composites.EMPTY;

            // If we're building a dense cell name, then we can directly allocate the
            // CellName object as it's complete.
            if (type instanceof CellNameType && ((CellNameType)type).isDense())
                return new SimpleDenseCellName(value);

            return new SimpleComposite(value);
        }
    }
}
