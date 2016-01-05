/*
 * Copyright 2014-2015 ScAi, CSD, UCLA.
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

import java.awt.Color;
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.ucla.cs.scai.clubsp.commons.BestSplitResult;
import edu.ucla.cs.scai.clubsp.commons.Range;
import edu.ucla.cs.scai.clubsp.commons.Sequence;
import edu.ucla.cs.scai.clubsp.commons.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class Clubs {

    private final JavaSparkContext ctx;
    int dimensionality;
    long initTime,
            topDownTime,
            intermediateRefinementTime,
            bottomUpTime,
            finalRefinementTime,
            totalTime;
    boolean drawCentroid, drawRadius;
    ArrayList<int[]> realCentroids;
    int nClustersAfterDivisivePhase, nClustersAfterIntermediateRefinment;
    ArrayList<ClusterSummary> clusterSummary;

    private static final double radiusMultiplier = 3;

    Sequence sequence = new Sequence();

    ClusterBlock root; //the root of the tree representing the partition
    ArrayList<ClusterBlock> clusters;
    Range domain;
    int dataSetSize, dataSetSizeWithoutOutliers;

    double SSQ, SSQ0, SSQ0withoutOutliers;
    double deltaSSQTot = 0;
    double totalConditionsWeight = 2;
    double BCSSQ = 0;
    double chIndex = 0;
    Color[] colbase = {Color.red, Color.green, Color.blue, Color.magenta, Color.cyan, Color.yellow, Color.gray, Color.orange};
    double hyperSphereConst = 0;
    String fileNameImages, fileNameLabels;

    JavaRDD<int[]> dataSet;
    JavaPairRDD<Integer, int[]> clusteredDataSet;

    HashMap<Integer, Color> colors = new HashMap<>();

    static final double[] hyperSphereConsts = {
            1, //0
            2, //1
            Math.PI, //2
            Math.PI * 4.0 / 3.0, //3
            Math.pow(Math.PI, 2) * 1.0 / 2.0, //4
            Math.pow(Math.PI, 2) * 8.0 / 15.0, //5
            Math.pow(Math.PI, 3) * 1.0 / 6.0, //6
            Math.pow(Math.PI, 3) * 16.0 / 105.0, //7
            Math.pow(Math.PI, 4) * 1.0 / 24.0, //8
            Math.pow(Math.PI, 4) * 32.0 / 945.0, //9
            Math.pow(Math.PI, 5) * 1.0 / 120.0, //10
    };

    public Clubs(JavaSparkContext ctx) {
        this.ctx=ctx;
    }

    public void enableImageCreation(String fileName) {
        this.fileNameImages = fileName;
    }

    public void disableImageCreation() {
        this.fileNameImages = null;
    }

    public boolean isImageCreationEnabled() {
        return fileNameImages != null;
    }

    public void enableLabelsCreation(String fileName) {
        this.fileNameLabels = fileName;
    }

    public void disableLabelsCreation() {
        this.fileNameLabels = null;
    }

    public void setDrawCentroid(boolean v) {
        drawCentroid = v;
    }

    public void setDrawRadius(boolean v) {
        drawRadius = v;
    }

    public boolean isLabelsCreationEnabled() {
        return fileNameLabels != null;
    }

    public long getInitTime() {
        return initTime;
    }

    public long getTopDownTime() {
        return topDownTime;
    }

    public long getIntermediateRefinementTime() {
        return intermediateRefinementTime;
    }

    public long getBottomUpTime() {
        return bottomUpTime;
    }

    public long getFinalRefinementTime() {
        return finalRefinementTime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void doClustering(String fileName) throws IOException {
        doClustering(fileName, 1);
    }

    public void doClustering(String fileName, final double scaleFactor) throws IOException {

        JavaRDD<String> lines = ctx.textFile(fileName);

        //load DataSetPoint form text file
        JavaRDD<int[]> dataSet = lines.map(new StringToIntArrayMapper(scaleFactor)).cache();

        doClustering(dataSet);
    }

    public void doClustering(JavaRDD<int[]> dataSet) {
        this.dataSet=dataSet;
        long startTime = System.currentTimeMillis();
        dimensionality=dataSet.first().length;
        ClusterBlock root = ClusterBlock.initRoot(dataSet);
        dataSetSize = root.n;
        initTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        topDownSplitting(root);
        topDownTime = System.currentTimeMillis() - startTime;
        nClustersAfterDivisivePhase = clusters.size();

        startTime = System.currentTimeMillis();
        intermediateRefinement();
        intermediateRefinementTime = System.currentTimeMillis() - startTime;
        nClustersAfterIntermediateRefinment = clusters.size();

        startTime = System.currentTimeMillis();
        bottomUpMerging();
        bottomUpTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        finalRefinement();
        if (fileNameLabels != null) {
            clusteredDataSet.map(new ClusteringToStringMapper()).saveAsTextFile(fileNameLabels);
        }
        finalRefinementTime = System.currentTimeMillis() - startTime;

        totalTime = initTime + topDownTime + intermediateRefinementTime + bottomUpTime + finalRefinementTime;
        System.out.println("Init time: ms " + initTime);
        System.out.println("Divisive time: ms " + topDownTime);
        System.out.println("Intermediate refinement time: ms " + intermediateRefinementTime);
        System.out.println("Agglomerative time: ms " + bottomUpTime);
        System.out.println("Final refinement time: ms " + finalRefinementTime);
        System.out.println("Total time: ms " + totalTime);
        System.out.println(nClustersAfterDivisivePhase + " clusters after divisive phase");
        System.out.println(nClustersAfterIntermediateRefinment + " clusters after intermediate refinement");
        System.out.println(clusterSummary.size() + " final clusters");
    }

    protected void topDownSplitting(ClusterBlock root) {

        //constructs a BSP
        clusters = new ArrayList<>();
        SSQ0 = root.SSQ;
        SSQ = SSQ0;
        PriorityQueue<ClusterBlock> q = new PriorityQueue<>();
        q.add(root);
        root.computeMarginalAvg();
        double maxCHIndex = 0;
        int k = 1;
        try {
            while (!q.isEmpty()) {
                ClusterBlock n = q.poll();
                //debug System.out.println("Iteration: " + iteration + " " + n.SSQ + "\t" + n.r);

                BestSplitResult bestSplit=n.computeBestSplit(0.05, 0.01);

                if (bestSplit.deltaSSQ==0) {
                    continue;
                }

                double newBCSSQ = BCSSQ + bestSplit.deltaSSQ;
                double newSSQ = SSQ - bestSplit.deltaSSQ;
                double newCHindex = (newBCSSQ * (root.n - (k + 1))) / (newSSQ * k); //k=k-1+1

                boolean isSplitEffective
                        = (k > 1 && newCHindex >= maxCHIndex)
                        || (newCHindex >= 0.7 * maxCHIndex && n.valleyCriterion(0.10));

                if (isSplitEffective) {
                    //System.out.println(n.r + "\t" + n.sd + "," + n.sp + "\t" + newCHindex);
                    n.split(bestSplit.bestDimension, bestSplit.bestPosition, sequence.next(), sequence.next());
                    SSQ = newSSQ;
                    n.actualDeltaSSQ=n.SSQ-n.lc.SSQ-n.rc.SSQ;
                    deltaSSQTot += n.actualDeltaSSQ;
                    BCSSQ = newBCSSQ;
                    chIndex = newCHindex;
                    maxCHIndex = Math.max(maxCHIndex, chIndex);
                    k++;
                    if (n.lc.SSQ>0) {
                        q.add(n.lc);
                    } else {
                        clusters.add(n.lc);
                    }
                    if (n.rc.SSQ>0) {
                        q.add(n.rc);
                    } else {
                        clusters.add(n.rc);
                    }
                } else {
                    n.lc = null;
                    n.rc = null;
                    n.sd = -1;
                    clusters.add(n);
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(Clubs.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private boolean highRestrictedDensity(ClusterBlock n, HashMap<Integer, double[]> originalCentroids, double density) {
        Range restrictedRange = n.r.getCopy();

        for (int j = 0; j < dimensionality; j++) {
            int restrictedWidth = Math.max(1, (int) ((n.r.sup[j] - n.r.inf[j] + 1) * 0.1 + 0.5));
            restrictedRange.sup[j] = Math.min(n.r.sup[j], (int) (originalCentroids.get(n.id)[j] + restrictedWidth));
            restrictedRange.inf[j] = Math.max(n.r.inf[j], (int) (originalCentroids.get(n.id)[j] - restrictedWidth));
            //restrictedRange.sup[j] = Math.min(n.r.sup[j], (int) (originalCentroids.get(n.id)[j] + 0.1 * originalDetailedRadii.get(n.id)[j] + 0.5));
            //restrictedRange.inf[j] = Math.max(n.r.inf[j], (int) (originalCentroids.get(n.id)[j] - 0.1 * originalDetailedRadii.get(n.id)[j] + 0.5));
        }

        JavaRDD<int[]> restrictedData=n.data.filter(new RestrictedRangeFilter(restrictedRange));
        long restrictedCount = restrictedData.count();

        double restrictedDensity = restrictedCount / restrictedRange.getVolume();
        return !(restrictedCount < 10 || restrictedDensity < 2 * density);
    }

    protected void intermediateRefinement() {
        HashMap<Integer, double[]> originalCentroids = new HashMap<>();
        HashMap<Integer, double[]> originalDetailedRadii = new HashMap<>();

        double sumOfDensities = 0;

        HashMap<Integer, Double> densities = new HashMap<>();
        ArrayList<Integer> sortedCounts = new ArrayList<>();

        double minDensity = Double.POSITIVE_INFINITY;
        for (ClusterBlock n : clusters) {
            double vol = n.r.getVolume();
            double d = n.n / vol;
            sortedCounts.add(n.n);
            sumOfDensities += d;
            densities.put(n.id, d);
            if (d < minDensity) {
                minDensity = d;
            }
        }

        Collections.sort(sortedCounts);

        for (ClusterBlock n : clusters) {
            double[] newCentroid = new double[dimensionality];
            int[] newInf = new int[dimensionality];
            int[] newSup = new int[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                newCentroid[i] = n.LS[i] / n.n;
                newInf[i] = Integer.MAX_VALUE;
                newSup[i] = Integer.MIN_VALUE;
            }
            originalCentroids.put(n.id, newCentroid);
            double[] detailedRadius = new double[dimensionality];
            double minDetailedRadius = Double.POSITIVE_INFINITY;
            for (int k = 0; k < dimensionality; k++) {
                double r = Math.sqrt(n.SSQd[k] / n.n);
                if (r == 0) {
                    continue;
                }
                detailedRadius[k] = radiusMultiplier * r;
                minDetailedRadius = Math.min(detailedRadius[k], minDetailedRadius);
            }
            originalDetailedRadii.put(n.id, detailedRadius);
        }

        //search for nodes containing only outliers
        //nodes with low density and low restricted density are considered to be outlier nodes
        ArrayList<Double> sortedDensities = new ArrayList<>(densities.values());
        Collections.sort(sortedDensities);
        double[] densityVariation = new double[sortedDensities.size() - 1];

        double ssd = 0;
        for (int i = 0; i < densityVariation.length; i++) {
            densityVariation[i] = sortedDensities.get(i + 1) / sortedDensities.get(i);
            ssd += densityVariation[i];
        }

        double asd = densityVariation.length > 0 ? ssd / densityVariation.length : 0;
        double dsd = 0;
        for (int i = 0; i < densityVariation.length; i++) {
            dsd += Math.pow(densityVariation[i] - asd, 2);
        }
        dsd /= densityVariation.length > 0 ? densityVariation.length : 1;
        dsd = Math.sqrt(dsd);

        double variationThreshold = asd + dsd;

        int indexThreshold = 0;

        while (indexThreshold < densityVariation.length && densityVariation[indexThreshold] <= variationThreshold) {
            indexThreshold++;
        }

        double densityThreshold = sortedDensities.get(indexThreshold);
        HashMap<Integer, Boolean> isOutlierClusterBlock = new HashMap<>();

        int countThreshold = 1; //(int) (Math.ceil(0.01 * dataSetSize / clusters.size()));

        for (ClusterBlock n : clusters) {
            boolean outlier = false;
            double density = densities.get(n.id);

            if (density <= densityThreshold) { //candidate for being an outlier node

                if (n.n >= countThreshold) {
                    outlier = !highRestrictedDensity(n, originalCentroids, density);
                } else {
                    outlier = true;
                }

                n.isOutlierNode = outlier;
            }
            isOutlierClusterBlock.put(n.id, outlier);
        }

        ArrayList<ClusterBlock> actualClusters=new ArrayList<>();
        for (ClusterBlock c : clusters) {
            c.isOutlierNode = isOutlierClusterBlock.get(c.id);
            if (!c.isOutlierNode) {
                actualClusters.add(c);
            }
        }

        HashMap<Integer, HashSet<Integer>> reachability = getReachability(clusters, actualClusters, originalCentroids, originalDetailedRadii);

        HashMap<Integer, ClusterSummary> globalClusterSummary=new HashMap<>();
        for (ClusterBlock n : clusters) {
            HashSet<Integer> reachableClusters = reachability.get(n.id);
            JavaRDD<HashMap<Integer, ClusterSummary>> clusterSummaryMapped = n.data.mapPartitions(new ClusterSummaryMapper(reachableClusters, originalCentroids, originalDetailedRadii));
            HashMap<Integer, ClusterSummary> partialClusterSummary= clusterSummaryMapped.reduce(new ClusterSummaryReducer());
            for (Map.Entry<Integer, ClusterSummary> e:partialClusterSummary.entrySet()) {
                if (globalClusterSummary.containsKey(e.getKey())) {
                    ClusterSummary gcs=globalClusterSummary.get(e.getKey());
                    gcs.add(e.getValue());
                } else {
                    globalClusterSummary.put(e.getKey(), e.getValue());
                }
            }
        }
        clusterSummary=new ArrayList(globalClusterSummary.values());
        for (ClusterSummary cs:clusterSummary) {
            cs.id=sequence.next();
        }

        SSQ=0;
        double[] LS = new double[dimensionality];
        double[] SS = new double[dimensionality];
        int n = 0;
        for (ClusterSummary cs : clusterSummary) {
            Utils.add(LS, cs.LS, 1);
            Utils.add(SS, cs.SS, 1);
            n += cs.n;
            SSQ+=cs.getSSQ();
        }
        dataSetSizeWithoutOutliers=n;
        SSQ0withoutOutliers = 0;
        for (int i = 0; i < dimensionality; i++) {
            SSQ0withoutOutliers += SS[i] - (LS[i] / n) * LS[i];
        }

        clusters=actualClusters;

        BCSSQ = SSQ0withoutOutliers - SSQ;
        //System.out.println("chIndex before: " + chIndex);
        if (clusters.size() > 1) {
            chIndex = (BCSSQ * (dataSetSizeWithoutOutliers - clusters.size())) / (SSQ * (clusters.size() - 1));
        }
        //System.out.println("chIndex after: " + chIndex);
        //createImageFromLeaves(fileName + "i2k", false, false, true, initialDomain);
    }

    protected void bottomUpMerging() {
        int k = clusterSummary.size();
        if (k <= 2) {
            return;
        }

        ArrayList<ClusterSummaryPair> merges = new ArrayList<>();
        double maxCHindex = chIndex;
        ClusterSummaryPairHeap q = new ClusterSummaryPairHeap(clusterSummary, sequence, false);
        if (q.isEmpty()) {
            return;
        }
        double minInc = q.peek().SSQinc;
        double newCHindex = ((BCSSQ - minInc) * (dataSetSizeWithoutOutliers - (k - 1))) / ((SSQ + minInc) * (k - 2));
        HashMap<Integer, ClusterSummary> clusterMap = new HashMap<>();
        for (ClusterSummary c : clusterSummary) {
            clusterMap.put(c.id, c);
        }
        double tempSSQ = SSQ;
        double tempBCSSQ = BCSSQ;
        while (newCHindex > maxCHindex * 0.9 && !q.isEmpty()) {
            //chIndex = newCHindex;
            tempSSQ += minInc;
            tempBCSSQ -= minInc;
            k--;
            maxCHindex = Math.max(maxCHindex, newCHindex); //max function needed because newIndex could be less than maxCHindex!
            merges.add(q.peek());
            //if (q.size() > 1) {
            Color c = colors.get(q.peek().c1.id);
            ClusterSummary mergedBlock = q.updateQueueByMerge();
            colors.put(mergedBlock.id, c);
            //}
            if (k > 2 && !q.isEmpty()) {
                minInc = q.peek().SSQinc;
                newCHindex = ((tempBCSSQ - minInc) * (dataSetSizeWithoutOutliers - (k - 1))) / ((tempSSQ + minInc) * (k - 2));
            } else {
                newCHindex = 0;
            }
        }

        if (merges.isEmpty()) {
            return;
        }
        Iterator<ClusterSummaryPair> it = merges.iterator();
        ClusterSummaryPair nextMerge = it.next();
        k = clusters.size();
        while (chIndex != maxCHindex) {
            clusterMap.remove(nextMerge.c1.id);
            clusterMap.remove(nextMerge.c2.id);
            clusterMap.put(nextMerge.cMerge.id, nextMerge.cMerge);
            SSQ += nextMerge.SSQinc;
            BCSSQ -= nextMerge.SSQinc;
            k--;
            chIndex = (BCSSQ * (dataSetSizeWithoutOutliers - k)) / (SSQ * (k - 1));
            if (!it.hasNext()) {
                break;
            }
            nextMerge = it.next();
        }

        //clusters = q.getClusters();
        clusterSummary = new ArrayList(clusterMap.values());

        //System.out.println("chIndex final: " + chIndex);
    }

    protected void finalRefinement() {
        clusteredDataSet = dataSet.mapToPair(new FinalRefinementMap(clusterSummary, radiusMultiplier));
    }

    public int numberOfClusters() throws Exception {
        return clusters.size();
    }

    private HashMap<Integer, HashSet<Integer>> getReachability(Collection<ClusterBlock> clusters, Collection<ClusterBlock> actualClusters, HashMap<Integer, double[]> centroids, HashMap<Integer, double[]> radii) {
        HashMap<Integer, HashSet<Integer>> res = new HashMap<>();

        for (ClusterBlock c1 : clusters) {
            HashSet<Integer> a = new HashSet<>();
            res.put(c1.id, a);
            for (ClusterBlock c2 : actualClusters) {
                if (c1.id == c2.id) {
                    a.add(c2.id);
                } else {
                    double[] nearestBorderPoint = new double[dimensionality];
                    double[] c = centroids.get(c2.id);
                    for (int i = 0; i < dimensionality; i++) {
                        if (c[i] >= c1.r.inf[i]) {
                            if (c[i] <= c1.r.sup[i]) {
                                nearestBorderPoint[i] = c[i];
                            } else {
                                nearestBorderPoint[i] = c1.r.sup[i];
                            }
                        } else {
                            nearestBorderPoint[i] = c1.r.inf[i];
                        }
                    }
                    if (Utils.ellipticalRelativeDistanceWithLimit1(c, radii.get(c2.id), nearestBorderPoint) <= 1) {
                        a.add(c2.id);
                    }
                }
            }
        }
        return res;
    }

    public int getNumberOfClusters() {
        return clusters.size();
    }

    public ArrayList<ClusterBlock> getClusters() {
        return clusters;
    }

    public void setRealCentroids(ArrayList<int[]> groundTruth) {
        this.realCentroids = groundTruth;
    }
}