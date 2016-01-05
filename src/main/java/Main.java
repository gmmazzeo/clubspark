import edu.ucla.cs.scai.clubsp.commons.MarginalDistribution;
import edu.ucla.cs.scai.clubsp.commons.Range;
import edu.ucla.cs.scai.clubsp.spark.Clubs;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

public final class Main {

    public static void main(String[] args) throws Exception {

        if (args==null || args.length==0) {
            System.out.println("File name missing");
            return;
        }
        String fileName=args[0];

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Clubspark");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        Clubs clubs=new Clubs(ctx);
        clubs.enableLabelsCreation(fileName+".labels");
        clubs.doClustering(fileName);
    }
}

