package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.MarginalDistribution;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by massimo on 11/19/15.
 */
public class SSMapper implements FlatMapFunction<Iterator<int[]>, double[]> {
    double[] SS;

    public SSMapper(int dimensionality) {
        SS=new double[dimensionality];
    }

    @Override
    public Iterable<double[]> call(Iterator<int[]> iterator) throws Exception {
        while (iterator.hasNext()) {
            int[] p=iterator.next();
            for (int i=0; i<p.length; i++) {
                SS[i]+=p[i]*p[i];
            }
        }
        ArrayList<double[]> res=new ArrayList<>();
        res.add(SS);
        return res;
    }

}
