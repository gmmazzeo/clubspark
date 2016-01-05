package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.Utils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * Created by massimo on 11/19/15.
 */
public class FinalRefinementMap implements PairFunction<int[], Integer, int[]> {
    ArrayList<double[]> centroids = new ArrayList<>();
    ArrayList<double[]> radii = new ArrayList<>();
    public FinalRefinementMap(Collection<ClusterSummary> clusterSummary, double radiusMultiplier) {
        for (ClusterSummary cs:clusterSummary) {
            centroids.add(cs.getCentroid());
            radii.add(Utils.multiply(Utils.copy(cs.getDetailedRadius()), radiusMultiplier));
        }
    }

    @Override
    public Tuple2<Integer, int[]> call(int[] p) throws Exception {
        int idNearestCluster=-1;
        //find the nearest cluster
        double minDist = Double.POSITIVE_INFINITY;
        for (int i=0; i<centroids.size(); i++) {
            double dist = Utils.ellipticalRelativeDistanceWithLimit1(centroids.get(i), radii.get(i), p);
            if (dist <= 1 && dist < minDist) {
                minDist = dist;
                idNearestCluster = i;
            }
        }
        return new Tuple2<>(idNearestCluster, p);
    }
}
