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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public interface Value
{
    public List<Term> asList();

    public static class MapLiteral extends HashMap<Term, Term> implements Value
    {
        public List<Term> asList()
        {
            List<Term> l = new ArrayList<Term>(2 * size());
            for (Map.Entry<Term, Term> entry : entrySet())
            {
                l.add(entry.getKey());
                l.add(entry.getValue());
            }
            return l;
        }
    }

    public static class ListLiteral extends ArrayList<Term> implements Value
    {
        public List<Term> asList()
        {
            return this;
        }
    }

    public static class SetLiteral extends HashSet<Term> implements Value
    {
        public List<Term> asList()
        {
            return new ArrayList<Term>(this);
        }
    }
}
