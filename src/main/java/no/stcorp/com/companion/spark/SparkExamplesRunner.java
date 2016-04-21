package no.stcorp.com.companion.spark;

import java.io.*;

import java.time.*;
import java.time.format.*;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.*;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

/** 
 * Run some Spark examples for checking if Spark functions as expected
 */ 
public class SparkExamplesRunner implements Serializable {
  private static final long serialVersionUID = 83748957589L;

  private static SparkConf mSparkConf;

  /**
   * Constructor
   * @param SparkContext The Spark context needed to load files, parallelize arrays, etc...
   */
	public SparkExamplesRunner(SparkConf pSparkConf) {
    mSparkConf = pSparkConf;
	}

	/**
	 * Run the spark examples
	 */
	public void run() {
    JavaSparkContext sc = new JavaSparkContext(mSparkConf); // JavaSparkContext object tells Spark how to access a cluster
    String logFile = "/home/osboxes/Tools/spark-1.5.1/README.md"; // Just an arbitrary file 
    /**
     * Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection 
     * of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing 
     * collection in your driver program, or referencing a dataset in an external storage system, such as a shared 
     * filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
     */
    JavaRDD<String> logData = sc.textFile(logFile).cache(); // Read the file logFile as a collection of lines (example of referencing a dataset in an external storage system). 
    System.out.println("Logdata lines: " + logData.count());

    // Find the number of lines containing the letters a and b using the filter transformation
    /**
     * All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just 
     * remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed 
     * when an action requires a result to be returned to the driver program.
     *
     * Spark’s API relies heavily on passing functions in the driver program to run on the cluster. In Java, functions 
     * are represented by classes implementing the interfaces in the org.apache.spark.api.java.function package. There 
     * are two ways to create such functions:
     * 1. Implement the Function interfaces in your own class, either as an anonymous inner class or a named one, and 
     *    pass an instance of it to Spark.
     * 2. In Java 8, use lambda expressions to concisely define an implementation.
     */
    long numAs = logData.filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = 51675674536574365L;
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = 69839048309483L;
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    // Just pause the application shortly to see the results of the previous operations
    try {
      System.out.println("Putting app to sleep for 1 second");
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      System.out.println("Something went wrong putting the app to sleep for 1 second");
      ex.printStackTrace();
      Thread.currentThread().interrupt();
    }

    System.out.println("Computing the sum of the elements in an array");
    int nrOfElements = 1000;
    Double[] doubleData = new Double[nrOfElements];
    for (int i=0; i<nrOfElements; i++) {
      doubleData[i] = i * Math.random();
    }

    // Compute the sum of a number of elements in a list
    List<Double> data = Arrays.asList(doubleData); 
    JavaRDD<Double> distData = sc.parallelize(data); // (Example of parallelizing an existing collection)
    System.out.println("Elements in parallelized collection: " + distData.count());
    double sum = distData.reduce((a, b) -> a + b); 
    System.out.println("Sum of elements: " + sum);

    try {
      System.out.println("Putting app to sleep for 1 second");
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      System.out.println("Something went wrong putting the app to sleep for 1 second");
      ex.printStackTrace();
      Thread.currentThread().interrupt();
    }
	}

  public void runSvm() {
    System.out.println("An example of a Support Vector Machine (SVM)");

    mSparkConf.setAppName("SVM Classifier Example");
    SparkContext sc = new SparkContext(mSparkConf);
    //String path = "/home/osboxes/Tools/spark-1.5.1/data/mllib/sample_libsvm_data.txt";
    String path = "/home/osboxes/Workspaces/simpleapp/mysvm.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // // Split initial RDD into two... [60% training data, 40% testing data]. Use seed for reproducibility.
    // JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
    // training.cache();
    // JavaRDD<LabeledPoint> test = data.subtract(training);

    // // Support vector machine
    // // Run training algorithm to build the model.
    // int numIterations = 100;
    // final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

    // // Clear the default threshold.
    // model.clearThreshold();

    // // Compute raw scores on the test set.
    // JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
    //   new Function<LabeledPoint, Tuple2<Object, Object>>() {
    //     public Tuple2<Object, Object> call(LabeledPoint p) {
    //       Double score = model.predict(p.features());
    //       System.out.println("Real value: " + p.label() + ", predicted: " + score);
    //       return new Tuple2<Object, Object>(score, p.label());
    //     }
    //   }
    // );

    // // Get evaluation metrics.
    // BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
    // double auROC = metrics.areaUnderROC();

    // System.out.println("=================================================");
    // System.out.println("Area under ROC = " + auROC);
    // System.out.println("=================================================");

    // // Save and load model
    Instant now = Instant.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").withZone(ZoneId.systemDefault());
    // String filePath = "model_SVM_" + formatter.format(now);
    // model.save(sc, filePath);
    // SVMModel sameModel = SVMModel.load(sc, filePath);


   // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> trainingLR = splits[0].cache();
    JavaRDD<LabeledPoint> testLR = splits[1];

    // Run training algorithm to build the model.
    final LogisticRegressionModel modelLR = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(trainingLR.rdd());

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testLR.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        private static final long serialVersionUID = 1298475632L;

        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double prediction = modelLR.predict(p.features());
          System.out.println("Real value: " + p.label() + ", predicted: " + prediction);
          return new Tuple2<Object, Object>(prediction, p.label());
        }
      }
    );

    // Get evaluation metrics.
    MulticlassMetrics metricsLR = new MulticlassMetrics(predictionAndLabels.rdd());
    double precision = metricsLR.precision();
    System.out.println("=================================================");
    System.out.println(modelLR.toString());
    System.out.println("Precision = " + precision);    
    System.out.println("=================================================");

    String filePath = "model_LR_" + formatter.format(now);
    modelLR.save(sc, filePath);
    LogisticRegressionModel sameModelLR = LogisticRegressionModel.load(sc, filePath);
    System.out.println("=================================================");
    System.out.println(sameModelLR.toString());
    System.out.println("=================================================");

    
    JavaRDD<LabeledPoint>[] tmp = data.randomSplit(new double[]{0.6, 0.4}, 12345);
    JavaRDD<LabeledPoint> trainingBayes = tmp[0]; // training set
    JavaRDD<LabeledPoint> testBayes = tmp[1]; // test set
    final NaiveBayesModel modelBayes = NaiveBayes.train(trainingBayes.rdd(), 1.0);
    JavaPairRDD<Double, Double> predictionAndLabel = testBayes.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
        private static final long serialVersionUID = 462802098378L;

        @Override
        public Tuple2<Double, Double> call(LabeledPoint p) {
          return new Tuple2<Double, Double>(modelBayes.predict(p.features()), p.label());
        }
      });
    double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
      private static final long serialVersionUID = 9488747847L;
    
      @Override
      public Boolean call(Tuple2<Double, Double> pl) {
        System.out.println("Actual: " + pl._1() + ", predicted: " + pl._2());
        return pl._1().equals(pl._2());
      }
    }).count() / (double) testBayes.count();
    System.out.println("=================================================");
    System.out.println(modelBayes.toString());
    System.out.println("Accuracy: " + accuracy);
    System.out.println("=================================================");
  }
	
}