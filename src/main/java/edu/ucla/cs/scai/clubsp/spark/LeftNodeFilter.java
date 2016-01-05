package edu.ucla.cs.scai.clubsp.spark;

import org.apache.spark.api.java.function.Function;

/**
 * Created by massimo on 11/19/15.
 */
public class LeftNodeFilter implements Function<int[], Boolean> {
    int sd, sp;
    public LeftNodeFilter(int sd, int sp) {
        this.sd=sd;
        this.sp=sp;
    }

    @Override
    public Boolean call(int[] p) throws Exception {
        return p[sd]<=sp;
    }
}
