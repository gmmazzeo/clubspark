package edu.ucla.cs.scai.clubsp.spark;

import org.apache.spark.api.java.function.Function;

/**
 * Created by massimo on 11/19/15.
 */
public class RightNodeFilter implements Function<int[], Boolean> {
    int sd, sp;
    public RightNodeFilter(int sd, int sp) {
        this.sd=sd;
        this.sp=sp;
    }

    @Override
    public Boolean call(int[] p) throws Exception {
        return p[sd]>sp;
    }
}
