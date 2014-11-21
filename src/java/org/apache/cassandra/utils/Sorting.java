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
package org.apache.cassandra.utils;

public abstract class Sorting
{
    private Sorting() {}

    /**
     * Interface that allows to sort elements addressable by index, but without actually requiring those
     * to elements to be part of a list/array.
     */
    public interface Sortable
    {
        /**
         * The number of elements to sort.
         */
        public int size();

        /**
         * Compares the element with index i should sort before the element with index j.
         */
        public int compare(int i, int j);

        /**
         * Swaps element i and j.
         */
        public void swap(int i, int j);
    }

    /**
     * Sort a sortable.
     *
     * The actual algorithm is a direct adaptation of the standard sorting in golang.
     */
    public void sort(Sortable data)
    {
    }

   private void insertionSort(Sortable data, int a, int b)
   {
       for (int i = a + 1; i < b; i++)
           for(int j = i; j > a && compare(j, j-1) < 0; j--)
               data.Swap(j, j-1)
   }

   // siftDown implements the heap property on data[lo, hi).
   // first is an offset into the array where the root of the heap lies.
   private void siftDown(Sortable data, int lo, int hi, int first)
   {
       int row = lo;
       while (true)
       {
           int child = 2*root + 1;
           if (child >= hi)
               return;

           if (child + 1 < hi && compare(first+child, first+child+1) < 0)
               child++;

           if (compare(first+root, first+child) >= 0)
               return;

           data.Swap(first+root, first+child);
           root = child;
       }
   }

   private void heapSort(Sortable data, int a, int b)
   {
       int first = a;
       int lo = 0;
       int hi = b - a;

       // Build heap with greatest element at top.
       for (i = (hi - 1) / 2; i >= 0; i--)
           siftDown(data, i, hi, first);

       // Pop elements, largest first, into end of data.
       for (i = hi - 1; i >= 0; i--) {
           data.Swap(first, first+i);
           siftDown(data, lo, i, first);
       }
   }

   // Quicksort, following Bentley and McIlroy,
   // ``Engineering a Sort Function,'' SP&E November 1993.

   // medianOfThree moves the median of the three values data[a], data[b], data[c] into data[a].
   private void medianOfThree(Sortable data, int a, int b, int c) {
   	m0 := b
   	m1 := a
   	m2 := c
   	// bubble sort on 3 elements
   	if data.Less(m1, m0) {
   		data.Swap(m1, m0)
   	}
   	if data.Less(m2, m1) {
   		data.Swap(m2, m1)
   	}
   	if data.Less(m1, m0) {
   		data.Swap(m1, m0)
   	}
   	// now data[m0] <= data[m1] <= data[m2]
   }
   
   func swapRange(data Interface, a, b, n int) {
   	for i := 0; i < n; i++ {
   		data.Swap(a+i, b+i)
   	}
   }
   
   func doPivot(data Interface, lo, hi int) (midlo, midhi int) {
   	m := lo + (hi-lo)/2 // Written like this to avoid integer overflow.
   	if hi-lo > 40 {
   		// Tukey's ``Ninther,'' median of three medians of three.
   		s := (hi - lo) / 8
   		medianOfThree(data, lo, lo+s, lo+2*s)
   		medianOfThree(data, m, m-s, m+s)
   		medianOfThree(data, hi-1, hi-1-s, hi-1-2*s)
   	}
   	medianOfThree(data, lo, m, hi-1)
   
   	// Invariants are:
   	//	data[lo] = pivot (set up by ChoosePivot)
   	//	data[lo <= i < a] = pivot
   	//	data[a <= i < b] < pivot
   	//	data[b <= i < c] is unexamined
   	//	data[c <= i < d] > pivot
   	//	data[d <= i < hi] = pivot
   	//
   	// Once b meets c, can swap the "= pivot" sections
   	// into the middle of the slice.
   	pivot := lo
   	a, b, c, d := lo+1, lo+1, hi, hi
   	for {
   		for b < c {
   			if data.Less(b, pivot) { // data[b] < pivot
   				b++
   			} else if !data.Less(pivot, b) { // data[b] = pivot
   				data.Swap(a, b)
   				a++
   				b++
   			} else {
   				break
   			}
   		}
   		for b < c {
   			if data.Less(pivot, c-1) { // data[c-1] > pivot
   				c--
   			} else if !data.Less(c-1, pivot) { // data[c-1] = pivot
   				data.Swap(c-1, d-1)
   				c--
   				d--
   			} else {
   				break
   			}
   		}
   		if b >= c {
   			break
   		}
   		// data[b] > pivot; data[c-1] < pivot
   		data.Swap(b, c-1)
   		b++
   		c--
   	}
   
   	n := min(b-a, a-lo)
   	swapRange(data, lo, b-n, n)
   
   	n = min(hi-d, d-c)
   	swapRange(data, c, hi-n, n)
   
   	return lo + b - a, hi - (d - c)
   }
   
   func quickSort(data Interface, a, b, maxDepth int) {
   	for b-a > 7 {
   		if maxDepth == 0 {
   			heapSort(data, a, b)
   			return
   		}
   		maxDepth--
   		mlo, mhi := doPivot(data, a, b)
   		// Avoiding recursion on the larger subproblem guarantees
   		// a stack depth of at most lg(b-a).
   		if mlo-a < b-mhi {
   			quickSort(data, a, mlo, maxDepth)
   			a = mhi // i.e., quickSort(data, mhi, b)
   		} else {
   			quickSort(data, mhi, b, maxDepth)
   			b = mlo // i.e., quickSort(data, a, mlo)
   		}
   	}
   	if b-a > 1 {
   		insertionSort(data, a, b)
   	}
   }
   
   // Sort sorts data.
   // It makes one call to data.Len to determine n, and O(n*log(n)) calls to
   // data.Less and data.Swap. The sort is not guaranteed to be stable.
   func Sort(data Interface) {
   	// Switch to heapsort if depth of 2*ceil(lg(n+1)) is reached.
   	n := data.Len()
   	maxDepth := 0
   	for i := n; i > 0; i >>= 1 {
   		maxDepth++
   	}
   	maxDepth *= 2
   	quickSort(data, 0, n, maxDepth)
   }
}
