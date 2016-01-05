package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.Utils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by massimo on 11/19/15.
 */
public class ClusterSummaryMapper implements FlatMapFunction<Iterator<int[]>, HashMap<Integer, ClusterSummary>> {
    HashSet<Integer> clusters;
    HashMap<Integer, double[]> centroids;
    HashMap<Integer, double[]> radii;
    HashMap<Integer, ClusterSummary> map =new HashMap<>();
    public ClusterSummaryMapper(HashSet<Integer> clusters, HashMap<Integer, double[]> centroids, HashMap<Integer, double[]> radii) {
        this.clusters=clusters;
        this.centroids=centroids;
        this.radii=radii;
    }

    @Override
    public Iterable<HashMap<Integer, ClusterSummary>> call(Iterator<int[]> iterator) throws Exception {
        while (iterator.hasNext()) {
            int[] p=iterator.next();
            int idNearestCluster=-1;
            //find the nearest cluster
            double minDist = Double.POSITIVE_INFINITY;
            for (int id : clusters) {
                double dist = Utils.ellipticalRelativeDistanceWithLimit1(centroids.get(id), radii.get(id), p);
                if (dist <= 1 && dist < minDist) {
                    minDist = dist;
                    idNearestCluster = id;
                }
            }
            if (idNearestCluster!=-1) {
                ClusterSummary cs=map.get(idNearestCluster);
                if (cs==null) {
                    cs=new ClusterSummary(p);
                    map.put(idNearestCluster, cs);
                } else {
                    cs.add(p);
                }
            }
        }
        ArrayList<HashMap<Integer, ClusterSummary>> res=new ArrayList<>();
        res.add(map);
        return res;
    }
}
