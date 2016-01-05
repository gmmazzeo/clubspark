package edu.ucla.cs.scai.clubsp.commons;

/**
 * Created by massimo on 11/19/15.
 */
public class BestSplitResult {

    public int bestDimension;
    public int bestPosition;
    public double deltaSSQ;

    public BestSplitResult(int bestDimension, int bestPosition, double deltaSSQ) {
        this.bestDimension = bestDimension;
        this.bestPosition = bestPosition;
        this.deltaSSQ = deltaSSQ;
    }
}
