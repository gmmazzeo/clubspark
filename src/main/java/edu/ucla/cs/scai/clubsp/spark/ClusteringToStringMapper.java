package edu.ucla.cs.scai.clubsp.spark;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by massimo on 12/7/15.
 */
public class ClusteringToStringMapper implements Function<Tuple2<Integer,int[]>, String> {

    @Override
    public String call(Tuple2<Integer, int[]> t) throws Exception {
        StringBuilder sb=new StringBuilder();
        for (int i=0; i<t._2().length; i++) {
            sb.append(t._2()[i]).append(" ");
        }
        sb.append(t._1());
        return sb.toString();
    }
}
