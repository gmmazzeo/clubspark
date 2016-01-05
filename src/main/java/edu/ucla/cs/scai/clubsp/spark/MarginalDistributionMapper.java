package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.MarginalDistribution;
import edu.ucla.cs.scai.clubsp.commons.Range;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by massimo on 11/18/15.
 */
class MarginalDistributionMapper implements FlatMapFunction<Iterator<int[]>, MarginalDistribution[]> {

    MarginalDistribution[] marginals;
    int[] inf;

    public MarginalDistributionMapper(Range domain) {
        inf=domain.inf;
        marginals=new MarginalDistribution[domain.getDimensionality()];
        for (int i=0; i<marginals.length; i++) {
            marginals[i]=new MarginalDistribution(domain.getWidth(i));
        }
    }

    @Override
    public Iterable<MarginalDistribution[]> call(Iterator<int[]> iterator) throws Exception {
        while (iterator.hasNext()) {
            int[] p=iterator.next();
            for (int i=0; i<p.length; i++) {
                marginals[i].addPoint(p, p[i]-inf[i]);
            }
        }
        ArrayList<MarginalDistribution[]> res=new ArrayList<>();
        res.add(marginals);
        return res;
    }
}
