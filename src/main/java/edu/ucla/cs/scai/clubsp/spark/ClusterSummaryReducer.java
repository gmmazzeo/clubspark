package edu.ucla.cs.scai.clubsp.spark;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by massimo on 11/19/15.
 */
public class ClusterSummaryReducer implements org.apache.spark.api.java.function.Function2<java.util.HashMap<Integer, ClusterSummary>, java.util.HashMap<Integer, ClusterSummary>, java.util.HashMap<Integer, ClusterSummary>> {

    @Override
    public HashMap<Integer, ClusterSummary> call(HashMap<Integer, ClusterSummary> map1, HashMap<Integer, ClusterSummary> map2) throws Exception {
        for (Map.Entry<Integer, ClusterSummary> e:map2.entrySet()) {
            ClusterSummary cs=map1.get(e.getKey());
            if (cs==null) {
                map1.put(e.getKey(), e.getValue());
            } else {
                cs.add(e.getValue());
            }
        }
        return map1;
    }
}
