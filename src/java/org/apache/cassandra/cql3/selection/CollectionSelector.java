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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.selection.Selection.ResultSetBuilder;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Selector class handling element (c[x]) and slice (c[x..y]) selections over collections.
 */
abstract class CollectionSelector extends Selector
{
    protected final Selector selected;

    protected CollectionSelector(Selector selected)
    {
        this.selected = selected;
    }

    // For sets and maps, return the type corresponding to the element of a selection (that is, x in c[x]).
    private static AbstractType<?> keyType(CollectionType<?> type)
    {
        return type.nameComparator();
    }

    // For sets and maps, return the type corresponding to the result of a selection (that is, c[x] in c[x]).
    public static AbstractType<?> valueType(CollectionType<?> type)
    {
        return type instanceof MapType ? type.valueComparator() : type.nameComparator();
    }

    private static abstract class AbstractFactory extends Factory
    {
        protected final String name;
        protected final Selector.Factory factory;
        protected final CollectionType<?> type;

        protected AbstractFactory(String name, Selector.Factory factory, CollectionType<?> type)
        {
            this.name = name;
            this.factory = factory;
            this.type = type;
        }

        protected String getColumnName()
        {
            return name;
        }

        protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
        {
            factory.addColumnMapping(mapping, resultsColumn);
        }

        public boolean isAggregateSelectorFactory()
        {
            return factory.isAggregateSelectorFactory();
        }
    }

    /**
     * Creates a {@code Selector.Factory} for the selection of an element of a collection.
     *
     * @param name a string representing the selection the factory is for. Something like "c[x]".
     * @param factory the {@code Selector.Factory} corresponding to the collection on which an element
     * is selected.
     * @param type the type of the collection.
     * @param key the element within the value represented by {@code factory} that is selected.
     * @return the created factory.
     */
    public static Factory newElementFactory(String name, Selector.Factory factory, CollectionType<?> type, final Term key)
    {
        return new AbstractFactory(name, factory, type)
        {
            protected AbstractType<?> getReturnType()
            {
                return valueType(type);
            }

            public Selector newInstance(QueryOptions options) throws InvalidRequestException
            {
                ByteBuffer keyValue = key.bindAndGet(options);
                if (keyValue == null)
                    throw new InvalidRequestException("Invalid null value for element selection on " + factory.getColumnName());
                if (keyValue == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    throw new InvalidRequestException("Invalid unset value for element selection on " + factory.getColumnName());
                return new ElementSelector(factory.newInstance(options), keyValue);
            }
        };
    }

    /**
     * Creates a {@code Selector.Factory} for the selection of a slice of a collection.
     *
     * @param name a string representing the selection the factory is for. Something like "c[x..y]".
     * @param factory the {@code Selector.Factory} corresponding to the collection on which a slice
     * is selected.
     * @param type the type of the collection.
     * @param from the starting bound of the selected slice. This cannot be {@code null} but can be
     * {@code Constants.UNSET_VALUE} if the slice doesn't have a start.
     * @param to the ending bound of the selected slice. This cannot be {@code null} but can be
     * {@code Constants.UNSET_VALUE} if the slice doesn't have an end.
     * @return the created factory.
     */
    public static Factory newSliceFactory(String name, Selector.Factory factory, CollectionType<?> type, final Term from, final Term to)
    {
        return new AbstractFactory(name, factory, type)
        {
            protected AbstractType<?> getReturnType()
            {
                return type;
            }

            public Selector newInstance(QueryOptions options) throws InvalidRequestException
            {
                ByteBuffer fromValue = from.bindAndGet(options);
                ByteBuffer toValue = to.bindAndGet(options);
                // Note that we use UNSET values to represent no bound, so null is truly invalid
                if (fromValue == null || toValue == null)
                    throw new InvalidRequestException("Invalid null value for slice selection on " + factory.getColumnName());
                return new SliceSelector(factory.newInstance(options), from.bindAndGet(options), to.bindAndGet(options));
            }
        };
    }

    public ByteBuffer getOutput(int protocolVersion) throws InvalidRequestException
    {
        ByteBuffer value = selected.getOutput(protocolVersion);
        return value == null ? null : extractSelection(value);
    }

    protected abstract ByteBuffer extractSelection(ByteBuffer collection);

    public void addInput(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
    {
        selected.addInput(protocolVersion, rs);
    }

    public void reset()
    {
        selected.reset();
    }

    private static class ElementSelector extends CollectionSelector
    {
        private final CollectionType<?> type;
        private final ByteBuffer key;

        private ElementSelector(Selector selected, ByteBuffer key)
        {
            super(selected);
            assert selected.getType() instanceof MapType || selected.getType() instanceof SetType : "this shouldn't have passed validation in Selectable";
            this.type = (CollectionType)selected.getType();
            this.key = key;
        }

        protected ByteBuffer extractSelection(ByteBuffer collection)
        {
            return type.getSerializer().getSerializedValue(collection, key, keyType(type));
        }

        public AbstractType<?> getType()
        {
            return valueType(type);
        }

        @Override
        public String toString()
        {
            return String.format("%s[%s]", selected, keyType(type).getString(key));
        }
    }

    private static class SliceSelector extends CollectionSelector
    {
        private final CollectionType<?> type;

        // Note that neither from nor to can be null, but they can both be ByteBufferUtil.UNSET_BYTE_BUFFER to represent no particular bound
        private final ByteBuffer from;
        private final ByteBuffer to;

        private SliceSelector(Selector selected, ByteBuffer from, ByteBuffer to)
        {
            super(selected);
            assert selected.getType() instanceof MapType || selected.getType() instanceof SetType : "this shouldn't have passed validation in Selectable";
            assert from != null && to != null : "We can have unset buffers, but not nulls";
            this.type = (CollectionType)selected.getType();
            this.from = from;
            this.to = to;
        }

        private boolean isUnset(ByteBuffer bb)
        {
            return bb == ByteBufferUtil.UNSET_BYTE_BUFFER;
        }

        protected ByteBuffer extractSelection(ByteBuffer collection)
        {
            return type.getSerializer().getSliceFromSerialized(collection, from, to, type.nameComparator());
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            boolean fromUnset = isUnset(from);
            boolean toUnset = isUnset(to);
            return fromUnset && toUnset
                 ? selected.toString()
                 : String.format("%s[%s..%s]", selected, fromUnset ? "" : keyType(type).getString(from), toUnset ? "" : keyType(type).getString(to));
        }
    }
}
