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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.cql3.selection.TermSelector;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A CQL3 term, i.e. a column value with or without bind variables.
 *
 * A Term can be either terminal or non terminal. A term object is one that is typed and is obtained
 * from a raw term (Term.Raw) by poviding the actual receiver to which the term is supposed to be a
 * value of.
 */
public interface Term
{
    /**
     * The names given to unamed bind markers found in selection. In selection clause, we often don't have a good
     * name for bind markers, typically if you have:
     *   SELECT (int)? FROM foo;
     * there isn't a good name for that marker. So we give the same name to all the markers. Note that we could try
     * to differenciate the names by using some increasing number in the name (so [selection_1], [selection_2], ...)
     * but it's actually not trivial to do in the current code and it's not really more helpful since if users wants
     * to bind by position (which they will have to in this case), they can do so at the driver level directly. And
     * so we don't bother.
     * Note that users should really be using named bind markers if they want to be able to bind by names.
     */
    public static final ColumnIdentifier bindMarkerNameInSelection = new ColumnIdentifier("[selection]", true);

    /**
     * Collects the column specification for the bind variables in this Term.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames);

    /**
     * Bind the values in this term to the values contained in {@code values}.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param options the values to bind markers to.
     * @return the result of binding all the variables of this NonTerminal (or
     * 'this' if the term is terminal).
     */
    public Terminal bind(QueryOptions options) throws InvalidRequestException;

    /**
     * A shorter for bind(values).get().
     * We expose it mainly because for constants it can avoids allocating a temporary
     * object between the bind and the get (note that we still want to be able
     * to separate bind and get for collections).
     */
    public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException;

    /**
     * Whether or not that term contains at least one bind marker.
     *
     * Note that this is slightly different from being or not a NonTerminal,
     * because calls to non pure functions will be NonTerminal (see #5616)
     * even if they don't have bind markers.
     */
    public abstract boolean containsBindMarker();

    public void addFunctionsTo(List<Function> functions);

    /**
     * A parsed, non prepared (thus untyped) term.
     *
     * This can be one of:
     *   - a constant
     *   - a collection literal
     *   - a function call
     *   - a marker
     */
    public abstract class Raw implements Selectable
    {
        /**
         * This method validates this RawTerm is valid for provided column
         * specification and "prepare" this RawTerm, returning the resulting
         * prepared Term.
         *
         * @param receiver the "column" this RawTerm is supposed to be a value of. Note
         * that the ColumnSpecification may not correspond to a real column in the
         * case this RawTerm describe a list index or a map key, etc...
         * @return the prepared term.
         */
        public abstract Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException;

        /**
         * The "default" type for the term, if any.
         * <p>
         * A CQL term doesn't necessarily have an intrinsic type. For instance, a bind marker '?' can
         * have any type depending on the context. Some terms do have fixed type (a casted marker for
         * instance, '(int)?', is of type int). And literal are "weakly" typed, i.e. are associated to
         * some types but not to only one. For instance, the literal '2' works for an int, a varint, a
         * float, etc.
         * This method will return their type for the terms that have a fixed one, will return
         * {@code null} if no type information can be determined from the term itself, and will a return
         * a "default" type for the weakly typed ones (the literals), which will be used when there is
         * no additional context.
         * The goal of this method is to provide a default type in selection clauses, i.e. when we have
         *   SELECT 1 FROM foo;
         * In that case, we need to have a concrete type for the returned value and we use this default
         * one (and if this method returns {@code null}, we reject the query asking for more type information).
         */
        public AbstractType<?> getDefaultType(String keyspace)
        {
            // Will be overriden when this make sense
            return getExactTypeIfKnown(keyspace);
        }

        public Selector.Factory newSelectorFactory(CFMetaData cfm, AbstractType<?> expectedType, List<ColumnDefinition> defs, VariableSpecifications boundNames) throws InvalidRequestException
        {
            /*
             * expectedType will be null if we have no constraint on what the type should be. For instance, if this term is a bind marker:
             *   - it will be null if we do "SELECT ? FROM foo"
             *   - it won't be null (and be LongType) if we do "SELECT bigintAsBlob(?) FROM foo" because the function constrain it.
             *
             * In the first case, we have to error out: we need to infer the type of the metadata of a SELECT at preparation time, which we can't
             * here (users will have to do "SELECT (varint)? FROM foo" for instance).
             * But in the 2nd case, we're fine and can use the expectedType to "prepare" the bind marker/collect the bound type.
             *
             * Further, the term might not be a bind marker, in which case we sometimes can default to some most-general type. For instance, in
             *   SELECT 3 FROM foo
             * we'll just default the type to 'varint' as that's the most generic type for the literal '3' (this is mostly for convenience, the query
             * is not terribly useful in practice and use can force the type as for the bind marker case through "SELECT (int)3 FROM foo").
             * But note that not all literals can have such default type. For instance, there is no way to infer the type of a UDT literal in a vacuum,
             * and so we simply error out if we have something like:
             *   SELECT { foo: 'bar' } FROM foo
             *
             * Lastly, note that if the term is a terminal literal, we don't have to check it's compatibility with 'expectedType' as any incompatibility
             * would have been found at preparation time.
             */
            AbstractType<?> type = expectedType;
            if (type == null)
            {
                type = getDefaultType(cfm.ksName);
                if (type == null)
                    throw new InvalidRequestException("Cannot infer type for term " + this + " in selection clause");
            }

            // The fact we default the name to "[selection]" inconditionally means that any bind marker in a
            // selection will have this name. Which isn't terribly helpful, but it's unclear how to provide
            // something a lot more helpful and in practice user can bind those markers by position or, even better,
            // use bind markers.
            Term term = prepare(cfm.ksName, new ColumnSpecification(cfm.ksName, cfm.cfName, bindMarkerNameInSelection, type));
            term.collectMarkerSpecification(boundNames);
            return TermSelector.newFactory(getText(), term, type);
        }

        /**
         * @return a String representation of the raw term that can be used when reconstructing a CQL query string.
         */
        public abstract String getText();

        @Override
        public String toString()
        {
            return getText();
        }
    }

    public abstract class MultiColumnRaw extends Term.Raw
    {
        public abstract Term prepare(String keyspace, List<? extends ColumnSpecification> receiver) throws InvalidRequestException;
    }

    /**
     * A terminal term, one that can be reduced to a byte buffer directly.
     *
     * This includes most terms that don't have a bind marker (an exception
     * being delayed call for non pure function that are NonTerminal even
     * if they don't have bind markers).
     *
     * This can be only one of:
     *   - a constant value
     *   - a collection value
     *
     * Note that a terminal term will always have been type checked, and thus
     * consumer can (and should) assume so.
     */
    public abstract class Terminal implements Term
    {
        public void collectMarkerSpecification(VariableSpecifications boundNames) {}
        public Terminal bind(QueryOptions options) { return this; }

        public void addFunctionsTo(List<Function> functions)
        {
        }

        // While some NonTerminal may not have bind markers, no Term can be Terminal
        // with a bind marker
        public boolean containsBindMarker()
        {
            return false;
        }

        /**
         * @return the serialized value of this terminal.
         * @param protocolVersion
         */
        public abstract ByteBuffer get(int protocolVersion) throws InvalidRequestException;

        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            return get(options.getProtocolVersion());
        }
    }

    public abstract class MultiItemTerminal extends Terminal
    {
        public abstract List<ByteBuffer> getElements();
    }

    /**
     * A non terminal term, i.e. a term that can only be reduce to a byte buffer
     * at execution time.
     *
     * We have the following type of NonTerminal:
     *   - marker for a constant value
     *   - marker for a collection value (list, set, map)
     *   - a function having bind marker
     *   - a non pure function (even if it doesn't have bind marker - see #5616)
     */
    public abstract class NonTerminal implements Term
    {
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            Terminal t = bind(options);
            return t == null ? null : t.get(options.getProtocolVersion());
        }
    }
}
