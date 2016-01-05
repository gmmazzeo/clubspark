/*
 * Copyright 2015 ScAi, CSD, UCLA.
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

import edu.ucla.cs.scai.clubsp.commons.BestSplitResult;
import edu.ucla.cs.scai.clubsp.commons.MarginalDistribution;
import edu.ucla.cs.scai.clubsp.commons.Range;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;

public class ClusterBlock implements Comparable<ClusterBlock> {
    int id;
    double[] LS; //linear sum of the coordinates of the global points in the block
    double[] SS;//squared sum of the coordinates of the global points in the block
    int n; //the number of global points inside the block
    Range r; //the coordinates of the global range
    ClusterBlock lc; //left child node
    ClusterBlock rc; //right child node
    ClusterBlock parent; //parent node
    ClusterBlock sibling; //sibling node
    int sd = -1; //the splitting dimension, when node is split sd is in [0..d-1];
    int sp; //the splitting position
    JavaRDD<int[]> data;
    double SSQ;
    double[] SSQd;
    double globalSSQd[];
    double globalRadius;
    double globalRadiusD[];
    double actualDeltaSSQ = 0;
    boolean isOutlierNode = false;
    MarginalDistribution[] marginals;
    int dimensionality;
    double avgMarg[];
    double ratioTestedPositions;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public static ClusterBlock initRoot(JavaRDD<int[]> data) {
        return new ClusterBlock(data, 0);
    }

    //data are scanned, global domain, marginals, LS, SS and SSQ are computed
    private ClusterBlock(JavaRDD<int[]> data, int id) {
        this.data = data;
        this.id = id;
        dimensionality=data.first().length;
        JavaRDD<Range> ranges=data.mapPartitions(new DomainMapper(dimensionality));
        r = ranges.reduce(new RangeReducer());
        computeMarginalsAndSummaryFromData();
    }

    //data are scanned, marginals, LS, SS and SSQ are computed
    private ClusterBlock(JavaRDD<int[]> data, Range r, int id) {
        this.data = data;
        this.r = r;
        this.id = id;
        dimensionality=r.inf.length;
        computeMarginalsAndSummaryFromData();
    }


    private void computeMarginalsAndSummaryFromData() {
        n=(int)data.count();
        JavaRDD<MarginalDistribution[]> partialMarginals = data.mapPartitions(new MarginalDistributionMapper(r));
        marginals = partialMarginals.reduce(new MarginaDistributionReducer());
        LS = new double[dimensionality];
        int shortestMarginal=0;
        for (int i=1; i<dimensionality; i++) {
            if (marginals[i].count.length<marginals[shortestMarginal].count.length) {
                shortestMarginal=i;
            }
        }
        for (int j = 0; j < marginals[shortestMarginal].count.length; j++) {
            if (marginals[shortestMarginal].count[j] > 0) {
                for (int i = 0; i < dimensionality; i++) {
                    LS[i] += marginals[shortestMarginal].sum[j][i];
                }
            }
        }
        JavaRDD<double[]> partSS=data.mapPartitions(new SSMapper(dimensionality));
        SS=partSS.reduce(new SSReducer());
        SSQd=new double[dimensionality];
        for (int i=0; i<dimensionality; i++) {
            SSQd[i]=SS[i]-(LS[i]/n)*LS[i];
            SSQ+=SSQd[i];
        }
    }

    public boolean isLeaf() {
        return lc == null;
    }

    public boolean isRoot() {
        return parent == null;
    }

    public double getSSQ() {
        return SSQ;
    }

    public void setIsOutlierNode(boolean isOutlierNode) {
        this.isOutlierNode = isOutlierNode;
    }

    public boolean isOutlierNode() {
        return isOutlierNode;
    }

    //compute the best split for the block
    public BestSplitResult computeBestSplit(double sampleStepRatio, double leastImprovementRatioPerStep) throws NullPointerException {
        double[] maxSsqReduction = new double[dimensionality]; //the maximum SSQ reduction found on each dimension
        int[] bestPosition = new int[dimensionality]; //the best position found on each dimension
        HashSet<Integer> improvableDimensions = new HashSet<>(); //the dimension which are currently considered improvable, i.e. the max SSQ reduction improved significantly in the last iteration

        for (int i = 0; i < dimensionality; i++) {
            improvableDimensions.add(i);
        }

        //compute the splitting position set
        int maxCard = 0; ///the maximum cardinality of the splitting position sets
        int totalSplittingPositions = 0;
        int testedSplittingPositions = 0;
        //splittingPosition are 0-based, i.e., they are translated by -inf
        MarginalDistribution[] cumulativeMarginals=new MarginalDistribution[dimensionality];
        ArrayList<Integer>[] splittingPositions = new ArrayList[dimensionality];
        for (int d = 0; d < dimensionality; d++) {
            cumulativeMarginals[d]=new MarginalDistribution(marginals[d].count.length);
            int lastCount=0;
            double[] lastSum=new double[dimensionality];
            splittingPositions[d] = new ArrayList<>();
            for (int j = 0; j < marginals[d].count.length; j++) {
                if (marginals[d].count[j] > 0) {
                    splittingPositions[d].add(j);
                    cumulativeMarginals[d].count[j]=lastCount+marginals[d].count[j];
                    cumulativeMarginals[d].sum[j]=new double[dimensionality];
                    for (int k=0; k<dimensionality; k++) {
                        cumulativeMarginals[d].sum[j][k]=lastSum[k]+marginals[d].sum[j][k];
                    }
                    lastCount=cumulativeMarginals[d].count[j];
                    lastSum=cumulativeMarginals[d].sum[j];
                }
            }

            Collections.shuffle(splittingPositions[d]);
            if (maxCard < splittingPositions[d].size()) {
                maxCard = splittingPositions[d].size();
            }
            totalSplittingPositions += splittingPositions[d].size();
        }

        int sampleStepSize = Math.max(1, (int) Math.ceil(sampleStepRatio * maxCard));
        int currentStep = 0;
        int maxNumberOfSteps = (int) Math.ceil(1.0 * maxCard / sampleStepSize);
        int positionIndex = 0;
        int[] lastPositionIndex = new int[dimensionality];

        while (!improvableDimensions.isEmpty() && currentStep < maxNumberOfSteps) {
            currentStep++;

            double[] newMaxSsqReduction = Arrays.copyOf(maxSsqReduction, maxSsqReduction.length);
            int[] newBestPosition = Arrays.copyOf(bestPosition, bestPosition.length);

            for (; positionIndex < Math.min(maxCard, currentStep * sampleStepSize); positionIndex++) {

                for (int i : improvableDimensions) { //scan the improvable dimensions and check the split on this dimension at the coordinate of the point p

                    if (positionIndex >= splittingPositions[i].size()) {
                        continue;
                    }
                    lastPositionIndex[i] = positionIndex;
                    int pos = splittingPositions[i].get(positionIndex);
                    int n1 = cumulativeMarginals[i].count[pos];
                    int n2 = n-n1;

                    double deltaSSQ = 0;
                    if (n1 * n2 != 0) {
                        for (int j = 0; j < dimensionality; j++) {
                            deltaSSQ += Math.pow(cumulativeMarginals[i].sum[pos][j] / n1 - (LS[j]-cumulativeMarginals[i].sum[pos][j]) / n2, 2);
                        }
                        deltaSSQ *= (1.0 * n1 / n) * n2;
                    }

                    if (deltaSSQ > newMaxSsqReduction[i]) {
                        newMaxSsqReduction[i] = deltaSSQ;
                        newBestPosition[i] = pos;
                    }

                    testedSplittingPositions++;
                }
            }

            for (Iterator<Integer> it = improvableDimensions.iterator(); it.hasNext();) {
                int i = it.next();
                if (currentStep != 1 && (newMaxSsqReduction[i] - maxSsqReduction[i]) / maxSsqReduction[i] < leastImprovementRatioPerStep) {
                    it.remove();
                }
                maxSsqReduction[i] = newMaxSsqReduction[i];
                bestPosition[i] = newBestPosition[i];
            }
        }

        int bestDim = -1;
        int bestPos = -1;

        double maxRed = 0;
        for (int i = 0; i < maxSsqReduction.length; i++) {
            if (maxSsqReduction[i] > maxRed) {
                maxRed = maxSsqReduction[i];
                bestDim = i;
                bestPos = bestPosition[i];
            }
        }

        if (bestDim == -1) {
            return null;
        }

        double ssqReductionThreshold = maxRed * 0.9;

        TreeSet<Integer> bestDimensions = new TreeSet<>(); //TreeSet guarantees that the dimensions are always processed with the same order
        for (int i = 0; i < maxSsqReduction.length; i++) {
            if (maxSsqReduction[i] >= ssqReductionThreshold) {
                bestDimensions.add(i);
            }
        }

        //now refine the position on the bestDimensions
        for (int i : bestDimensions) {
            for (positionIndex = lastPositionIndex[i] + 1; positionIndex < splittingPositions[i].size(); positionIndex++) {
                int pos = splittingPositions[i].get(positionIndex);
                int n1 = cumulativeMarginals[i].count[pos];
                int n2 = n-n1;

                double deltaSSQ = 0;
                if (n1 * n2 != 0) {
                    for (int j = 0; j < dimensionality; j++) {
                        deltaSSQ += Math.pow(cumulativeMarginals[i].sum[pos][j] / n1 - (LS[j] - cumulativeMarginals[i].sum[pos][j]) / n2, 2);
                    }
                    deltaSSQ *= (1.0 * n1 / n) * n2;
                }
                if (deltaSSQ >= maxSsqReduction[i]) { //don't remove the =, it is needed to guarantee always the same split dimensions
                    maxSsqReduction[i] = deltaSSQ;
                    if (deltaSSQ >= maxRed) { //don't remove the =, it is needed to guarantee always the same split dimensions
                        maxRed = deltaSSQ;
                        bestDim = i;
                        bestPos = pos;
                    }
                }

                testedSplittingPositions++;

            }
        }

        ratioTestedPositions = 1.0 * testedSplittingPositions / totalSplittingPositions;

        return new BestSplitResult(bestDim, bestPos+r.getInfCoord(bestDim), maxSsqReduction[bestDim]);
    }

    //splits the block, partition data and computes the marginals, LS, and SS
    //this method is for the worker
    public void split(int splitDimension, int splitPosition, int leftId, int rightId) {
        Range[] newRanges = r.getSplit(splitDimension, splitPosition);
        JavaRDD<int[]> dataLeft=data.filter(new LeftNodeFilter(splitDimension, splitPosition)).cache();
        JavaRDD<int[]> dataRight =data.filter(new RightNodeFilter(splitDimension, splitPosition)).cache();
        if (!isRoot()) {
            data.unpersist(); //will this work???
        }
        sd = splitDimension;
        sp = splitPosition;
        ClusterBlock leftBlock = new ClusterBlock(dataLeft, newRanges[0], leftId);
        ClusterBlock rightBlock = new ClusterBlock(dataRight, newRanges[1], rightId);
        leftBlock.parent = this;
        rightBlock.parent = this;
        lc = leftBlock;
        rc = rightBlock;
        leftBlock.sibling = rightBlock;
        rightBlock.sibling = leftBlock;
    }

    @Override
    public int compareTo(ClusterBlock cb) {
        return Double.compare(cb.SSQ, SSQ);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterBlock that = (ClusterBlock) o;

        return id == that.id;

    }

    @Override
    public int hashCode() {
        return id;
    }

    public Range getR() {
        return r;
    }

    public double[] getLS() {
        return LS;
    }

    public void setLS(double[] LS) {
        this.LS = LS;
    }

    public double[] getSS() {
        return SS;
    }

    public void setSS(double[] SS) {
        this.SS = SS;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double[] getCentroid() {
        double[] centroid = new double[dimensionality];
        for (int k = 0; k < dimensionality; k++) {
            centroid[k] = LS[k] / n;
        }
        return centroid;
    }

    public MarginalDistribution[] getMarginals() {
        return marginals;
    }

    public boolean valleyCriterion(double delta) {

        for (int dimension=0; dimension<dimensionality; dimension++) {
            int width = r.sup[dimension] - r.inf[dimension] + 1;
            int windowSemiWidth = width / 20;
            if (windowSemiWidth < 2) {
                return false;
            }

            double[] mobileAvg = new double[width];
            int currentWindowsCount = 0;
            for (int j = 0; j < windowSemiWidth; j++) {
                currentWindowsCount += marginals[dimension].count[j];
            }
            int currentWindowSize = windowSemiWidth;
            double maxMobileAvg = mobileAvg[0];
            double minMobileAvg = mobileAvg[1];

            double sum = 0;
            //double sumSqr = 0;

            for (int j = 0; j < width; j++) {
                mobileAvg[j] = 1.0 * currentWindowsCount / currentWindowSize;
                sum += mobileAvg[j];
                //sumSqr += mobileAvg[j] * mobileAvg[j];
                if (mobileAvg[j] > maxMobileAvg) {
                    maxMobileAvg = mobileAvg[j];
                } else if (mobileAvg[j] < minMobileAvg) {
                    minMobileAvg = mobileAvg[j];
                }
                if (j >= windowSemiWidth) {
                    currentWindowsCount -= marginals[dimension].count[j - windowSemiWidth];
                    currentWindowSize--;
                }
                if (j + windowSemiWidth < mobileAvg.length) {
                    currentWindowsCount += marginals[dimension].count[j + windowSemiWidth];
                    currentWindowSize++;
                }
            }

            double avg = sum / width;
            double gap = avg * delta;
            if (gap < 1) {
                gap = 1;
            }

            ArrayList<Integer> restrictedPositions = new ArrayList<>();
            //now find local minima and maxima
            boolean[] localMinima = new boolean[width];
            boolean[] localMaxima = new boolean[width];
            int lastRestrictedPositionType = 0; //-1: min, 1: max

            if (parent != null && maxMobileAvg < 1.0 * n / r.getWidth(dimension)) {
                return false;
            }

            double maxInfLimit = minMobileAvg + delta * avg;
            double minSupLimit = maxMobileAvg - delta * avg;

            if (mobileAvg[0] < mobileAvg[1] && mobileAvg[0] <= minSupLimit) {
                localMinima[0] = true;
            } else if (mobileAvg[0] > mobileAvg[1] && mobileAvg[0] >= maxInfLimit) {
                localMaxima[0] = true;
                lastRestrictedPositionType = 1;
                restrictedPositions.add(0);
            }

            for (int j = 1; j < width - 1; j++) {
                if (mobileAvg[j] < mobileAvg[j - 1] && mobileAvg[j] < mobileAvg[j + 1] && mobileAvg[j] <= minSupLimit) {
                    localMinima[j] = true;
                    if (lastRestrictedPositionType == 1) {
                        restrictedPositions.add(j);
                        lastRestrictedPositionType = -1;
                    } else if (lastRestrictedPositionType == -1 && mobileAvg[j] < mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                        restrictedPositions.set(restrictedPositions.size() - 1, j);
                    }
                } else if (mobileAvg[j] > mobileAvg[j - 1] && mobileAvg[j] > mobileAvg[j + 1] && mobileAvg[j] >= maxInfLimit) {
                    localMaxima[j] = true;
                    if (lastRestrictedPositionType != 1) {
                        restrictedPositions.add(j);
                        lastRestrictedPositionType = 1;
                    } else if (lastRestrictedPositionType == 1 && mobileAvg[j] > mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                        restrictedPositions.set(restrictedPositions.size() - 1, j);
                    }
                }
            }

            if (mobileAvg[width - 1] < mobileAvg[width - 2] && mobileAvg[width - 1] <= minSupLimit) {
                localMinima[width - 1] = true;
                if (lastRestrictedPositionType == 1) {
                    restrictedPositions.add(width - 1);
                } else if (lastRestrictedPositionType == -1 && mobileAvg[width - 1] < mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                    restrictedPositions.set(restrictedPositions.size() - 1, width - 1);
                }
            } else if (mobileAvg[width - 1] > mobileAvg[width - 2] && mobileAvg[width - 1] >= maxInfLimit) {
                localMaxima[width - 1] = true;
                if (lastRestrictedPositionType != 1) {
                    restrictedPositions.add(width - 1);
                } else if (lastRestrictedPositionType == 1 && mobileAvg[width - 1] > mobileAvg[restrictedPositions.get(restrictedPositions.size() - 1)]) {
                    restrictedPositions.set(restrictedPositions.size() - 1, width - 1);
                }
            }
            if (restrictedPositions.size() < 3) {
                return false;
            }
            //find i
            int i = 0;

            double minSup = mobileAvg[restrictedPositions.get(0)] - gap;
            //find j
            int j = 1;
            while (j < restrictedPositions.size() && (localMaxima[restrictedPositions.get(j)] || mobileAvg[restrictedPositions.get(j)] > minSup)) {
                if (mobileAvg[restrictedPositions.get(j)] > mobileAvg[restrictedPositions.get(i)]) {
                    i = j;
                    minSup = mobileAvg[restrictedPositions.get(i)] - gap;
                }
                j++;
            }
            if (j == restrictedPositions.size()) {
                return false;
            }
            double maxInf = mobileAvg[restrictedPositions.get(j)] + gap;
            //find k
            int k = j + 1;
            while (k < restrictedPositions.size() && (localMinima[restrictedPositions.get(k)] || mobileAvg[restrictedPositions.get(k)] < maxInf)) {
                if (mobileAvg[restrictedPositions.get(k)] < mobileAvg[restrictedPositions.get(j)]) {
                    j = k;
                    maxInf = mobileAvg[restrictedPositions.get(j)] + gap;
                }
                k++;
            }
            if (k < restrictedPositions.size()) {
                return true;
            }
        }
        return false;
    }

    public int rangeCount(Range range) {
        return (int)data.filter(p -> new Boolean(range.contains(p))).count();
    }

    public void computeMarginalAvg() {
        avgMarg = new double[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            avgMarg[i] = 1.0 * n / r.getWidth(i);
        }
    }
}
