package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.MarginalDistribution;
import org.apache.spark.api.java.function.Function2;

public class MarginaDistributionReducer implements Function2<MarginalDistribution[], MarginalDistribution[], MarginalDistribution[]> {
    @Override
    public MarginalDistribution[] call(MarginalDistribution[] m1, MarginalDistribution[] m2) throws Exception {
        for (int i=0; i<m1.length; i++) {
            m1[i].add(m2[i]);
        }
        return m1;
    }
}