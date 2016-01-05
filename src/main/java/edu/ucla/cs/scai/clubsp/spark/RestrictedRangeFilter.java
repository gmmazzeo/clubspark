package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.Range;
import org.apache.spark.api.java.function.Function;

/**
 * Created by massimo on 12/2/15.
 */
public class RestrictedRangeFilter implements Function<int[], Boolean> {
    Range restrictedRange;
    public RestrictedRangeFilter(Range restrictedRange) {
        this.restrictedRange=restrictedRange;
    }

    @Override
    public Boolean call(int[] p) throws Exception {
        return restrictedRange.contains(p);
    }
}
