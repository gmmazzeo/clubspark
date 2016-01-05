package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.Range;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by massimo on 12/4/15.
 */
public class DomainMapper implements FlatMapFunction<Iterator<int[]>, Range> {

    Range r;
    public DomainMapper(int dimensionality) {
        int[] inf=new int[dimensionality];
        int[] sup=new int[dimensionality];
        r=new Range(inf, sup);
        for (int i=0; i<dimensionality; i++) {
            inf[i]=Integer.MAX_VALUE;
            sup[i]=Integer.MIN_VALUE;
        }
    }

    @Override
    public Iterable<Range> call(Iterator<int[]> iterator) throws Exception {
        while (iterator.hasNext()) {
            int[] p=iterator.next();
            for (int i=0; i<p.length; i++) {
                if (p[i]<r.inf[i]) {
                    r.inf[i]=p[i];
                }
                if (p[i]>r.sup[i]) {
                    r.sup[i]=p[i];
                }
            }
        }
        ArrayList<Range> res=new ArrayList<>();
        res.add(r);
        return res;
    }
}
