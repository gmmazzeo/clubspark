package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.Range;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by massimo on 12/4/15.
 */
public class RangeReducer implements Function2<Range, Range, Range> {

    @Override
    public Range call(Range r1, Range r2) throws Exception {
        for (int i=0; i<r1.inf.length; i++) {
            if (r2.inf[i] < r1.inf[i]) {
                r1.inf[i]=r2.inf[i];
            }
            if (r2.inf[i] < r1.inf[i]) {
                r1.inf[i]=r2.inf[i];
            }
        }
        return r1;
    }
}
