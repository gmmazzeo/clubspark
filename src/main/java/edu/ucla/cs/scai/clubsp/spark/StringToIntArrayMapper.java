package edu.ucla.cs.scai.clubsp.spark;

import org.apache.spark.api.java.function.Function;

import java.util.StringTokenizer;

/**
 * Created by massimo on 12/4/15.
 */
public class StringToIntArrayMapper implements Function<String, int[]> {

    double scaleFactor;

    public StringToIntArrayMapper(double scaleFactor) {
        this.scaleFactor=scaleFactor;
    }

    @Override
    public int[] call(String s) {
        StringTokenizer st=new StringTokenizer(s, "\t ,");
        int[] p=new int[st.countTokens()];
        for (int i=0; i<p.length; i++) {
            p[i]=(int)(0.5+scaleFactor*Integer.parseInt(st.nextToken()));
        }
        return p;
    }
}
