/*
 * Copyright 2014 ScAi, CSD, UCLA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.Sequence;

import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class ClusterSummaryPairHeap {

    ClusterSummaryPair[] heap;
    int size = 0;
    int nClusters = 0;
    boolean ignoreRadiusCriterion;
    Sequence sequence;

    public ClusterSummaryPairHeap(ArrayList<ClusterSummary> clusters, Sequence sequence, boolean ignoreRadiusCriterion) {
        this.sequence=sequence;
        this.ignoreRadiusCriterion = ignoreRadiusCriterion;
        nClusters = clusters.size();
        ArrayList<ClusterSummaryPair> admissiblePairs = new ArrayList<>();
        for (int i = 0; i < nClusters; i++) {
            for (int j = i + 1; j < nClusters; j++) {
                ClusterSummaryPair cp = new ClusterSummaryPair(clusters.get(i), clusters.get(j), ignoreRadiusCriterion);
                if (cp.SSQinc != Double.POSITIVE_INFINITY) {
                    admissiblePairs.add(cp);
                }
            }
        }
        heap = new ClusterSummaryPair[admissiblePairs.size()];
        for (ClusterSummaryPair cp : admissiblePairs) {
            //System.out.println(cp.c1.id+"\t"+cp.c2.id);
            heap[size] = cp;
            moveUp(size);
            size++;
        }
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public ClusterSummaryPair peek() {
        return heap[0];
    }

    public ClusterSummary updateQueueByMerge() {
        ClusterSummaryPair cp0 = heap[0];
        int id1 = cp0.c1.id;
        int id2 = cp0.c2.id;
        int insertIndex = 0;
        int readIndex = 1;
        int oldSize = size;
        ClusterSummary c = cp0.getMerge(); //c is the merge of id1 and id2
        c.id=sequence.next();
        ArrayList<ClusterSummaryPair> newPairs = new ArrayList<>();
        size--;
        while (readIndex < oldSize) {
            if (heap[readIndex].c1.id == id1 || heap[readIndex].c2.id == id1
                    || heap[readIndex].c1.id == id2 || heap[readIndex].c2.id == id2) {
                size--;
                if (heap[readIndex].c1.id == id1) { //1st cluster of the pair is id1
                    ClusterSummaryPair cp = new ClusterSummaryPair(c, heap[readIndex].c2, ignoreRadiusCriterion); //replace the pair with the merge of c and the 2nd cluster of the pair
                    if (cp.SSQinc != Double.POSITIVE_INFINITY) {
                        newPairs.add(cp);
                    }
                } else if (heap[readIndex].c2.id == id1) { //2nd cluster of the pair is id1
                    ClusterSummaryPair cp = new ClusterSummaryPair(c, heap[readIndex].c1, ignoreRadiusCriterion); //replace the pair with the merge of c and the 1st cluster of the pair
                    if (cp.SSQinc != Double.POSITIVE_INFINITY) {
                        newPairs.add(cp);
                    }
                } else if (heap[readIndex].c1.id == id2) { //1st cluster of the pair is id2
                    ClusterSummaryPair cp = new ClusterSummaryPair(c, heap[readIndex].c2, ignoreRadiusCriterion); //replace the pair with the merge of c and the 2nd cluster of the pair
                    if (cp.SSQinc != Double.POSITIVE_INFINITY) {
                        newPairs.add(cp);
                    }
                } else if (heap[readIndex].c2.id == id2) { //2nd cluster of the pair is id2
                    ClusterSummaryPair cp = new ClusterSummaryPair(c, heap[readIndex].c1, ignoreRadiusCriterion); //replace the pair with the merge of c and the 1st cluster of the pair
                    if (cp.SSQinc != Double.POSITIVE_INFINITY) {
                        newPairs.add(cp);
                    }
                }
            } else {
                heap[insertIndex] = heap[readIndex];
                insertIndex++;
            }
            readIndex++;
        }
        for (int i = size; i < heap.length; i++) {
            heap[i] = null;
        }

        for (int i = 1; i < size; i++) { //heapify
            moveUp(i);
        }

        for (ClusterSummaryPair cp : newPairs) {
            heap[size] = cp;
            moveUp(size);
            size++;
        }
        return c;
    }

    public ArrayList<ClusterSummary> getClusters() {
        HashSet<Integer> processed = new HashSet<>();
        ArrayList<ClusterSummary> res = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (!processed.contains(heap[i].c1.id)) {
                res.add(heap[i].c1);
                processed.add(heap[i].c1.id);
            }
            if (!processed.contains(heap[i].c2.id)) {
                res.add(heap[i].c2);
                processed.add(heap[i].c2.id);
            }
        }
        return res;
    }

    private int moveUp(int p) {
        while (p > 0 && heap[p].compareTo(heap[(p + 1) / 2 - 1]) < 0) {
            ClusterSummaryPair tmp = heap[(p + 1) / 2 - 1];
            heap[(p + 1) / 2 - 1] = heap[p];
            heap[p] = tmp;
            p = (p + 1) / 2 - 1;
        }
        return p;
    }

    private int minChild(int p) {
        int p1 = (p + 1) * 2 - 1;
        int p2 = (p + 1) * 2;
        if (p1 >= size) {
            return p;
        }
        if (p2 == size) {
            p2 = p1;
        }
        if (heap[p2].compareTo(heap[p1]) < 0) {
            if (heap[p2].compareTo(heap[p]) < 0) {
                return p2;
            } else {
                return p;
            }
        } else {
            if (heap[p1].compareTo(heap[p]) < 0) {
                return p1;
            } else {
                return p;
            }
        }
    }
}
