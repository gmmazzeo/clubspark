package edu.ucla.cs.scai.clubsp.spark;

import java.io.Serializable;

/**
 * Created by massimo on 11/19/15.
 */
public class ClusterSummary implements Serializable {
    int n;
    double[] LS;
    double[] SS;
    Double SSQ;
    double[] SSQd;
    double radius;
    double[] detailedRadius;
    double[] centroid;
    int id;
    boolean modified=true;

    public ClusterSummary(int n, double[] LS, double[] SS) {
        this.n=n;
        this.LS=LS;
        this.SS=SS;
    }

    public ClusterSummary(int[] p) {
        this.n = 1;
        this.LS = new double[p.length];
        this.SS = new double[p.length];
        for (int i=0; i<p.length; i++) {
            LS[i]=p[i];
            SS[i]=p[i]*p[i];
        }
    }

    public void add(int[] p) {
        n++;
        for (int i=0; i<p.length; i++) {
            LS[i]+=p[i];
            SS[i]+=p[i]*p[i];
        }
        modified=true;
    }

    public void add(ClusterSummary cs) {
        n+=cs.n;
        for (int i=0; i<LS.length; i++) {
            LS[i]+=cs.LS[i];
            SS[i]+=cs.SS[i];
        }
        modified=true;
    }

    public double getSSQ() {
        if (modified) {
            computeFunctions();
        }
        return SSQ;
    }

    public double[] getSSQd() {
        if (modified) {
            computeFunctions();
        }
        return SSQd;
    }

    public void computeFunctions() {
        SSQd=new double[LS.length];
        SSQ=0d;
        centroid=new double[LS.length];
        detailedRadius=new double[LS.length];
        for (int i=0; i<LS.length; i++) {
            centroid[i]=LS[i]/n;
            SSQd[i]=(SS[i]-(centroid[i])*LS[i]);
            SSQ+=SSQd[i];
            detailedRadius[i]=Math.sqrt(SSQd[i] / n);
        }
        radius=Math.sqrt(SSQ/n);
    }

    public double[] getDetailedRadius() {
        if (modified) {
            computeFunctions();
        }
        return detailedRadius;
    }

    public double getRadius(int i) {
        if (modified) {
            computeFunctions();
        }
        return radius;
    }

    public double[] getCentroid() {
        if (modified) {
            computeFunctions();
        }
        return centroid;
    }
}
