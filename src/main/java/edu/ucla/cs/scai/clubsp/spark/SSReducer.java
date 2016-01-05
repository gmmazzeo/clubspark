package edu.ucla.cs.scai.clubsp.spark;

import edu.ucla.cs.scai.clubsp.commons.MarginalDistribution;
import org.apache.spark.api.java.function.Function2;

public class SSReducer implements Function2<double[], double[], double[]> {
    @Override
    public double[] call(double[] m1, double[] m2) throws Exception {
        for (int i=0; i<m1.length; i++) {
            m1[i]+=m2[i];
        }
        return m1;
    }
}