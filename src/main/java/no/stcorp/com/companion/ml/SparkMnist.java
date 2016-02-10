package no.stcorp.com.companion.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.canova.api.conf.Configuration;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.SVMLightRecordReader;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.ConvolutionLayer;
import org.deeplearning4j.nn.conf.layers.RBM;
import org.deeplearning4j.nn.conf.layers.SubsamplingLayer;
import org.deeplearning4j.nn.conf.layers.setup.ConvolutionLayerSetup;
import org.deeplearning4j.nn.conf.override.ConfOverride;
import org.deeplearning4j.nn.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.canova.RecordReaderFunction;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.*;
import java.util.Collections;

/**
 * Test using deeplearning4j library
 */
public class SparkMnist implements Serializable {
  private static final long serialVersionUID = 1L;

  private static JavaSparkContext mSparkContext;
  
  /**
   * Constructor
   * @param pSparkContext The Spark context needed for example to run the machine learning algorithm
   */
  public SparkMnist(SparkConf pSparkConf) {
    mSparkContext = new JavaSparkContext(pSparkConf); // JavaSparkContext object tells Spark how to access a cluster
  }

  public void runSVM() throws Exception {
    final int numRows = 28;
    final int numColumns = 28;
    int outputNum = 10;
    int numSamples = 600;
    int nChannels = 1;
    int batchSize = 10;
    int iterations = 10;
    int seed = 123;
    int listenerFreq = batchSize / 5;

    MultiLayerConfiguration.Builder builder = new NeuralNetConfiguration.Builder()
            .seed(seed)
            //.batchSize(batchSize)
            .iterations(iterations)
            .constrainGradientToUnitNorm(true)
            .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
            .list(3)
            .layer(0, new ConvolutionLayer.Builder(10, 10)
                    .nIn(nChannels)
                    .nOut(6)
                    .weightInit(WeightInit.XAVIER)
                    .activation("relu")
                    .build())
            .layer(1, new SubsamplingLayer.Builder(SubsamplingLayer.PoolingType.MAX, new int[] {2,2})
                    .build())
            .layer(2, new org.deeplearning4j.nn.conf.layers.OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                    .nIn(150)
                    .nOut(outputNum)
                    .weightInit(WeightInit.XAVIER)
                    .activation("softmax")
                    .build())
            .backprop(true).pretrain(false);
    new ConvolutionLayerSetup(builder,28,28,1);

    MultiLayerConfiguration conf = builder.build();
    MultiLayerNetwork network = new MultiLayerNetwork(conf);
    network.init();

    System.out.println("Initializing network");
    // Create Spark mullti layer network from configuration
    System.out.println("Spark context: " + mSparkContext + ", multi layer configuration: " + conf);
    SparkDl4jMultiLayer master = new SparkDl4jMultiLayer(mSparkContext, conf);
    //number of partitions should be partitioned by batch size
    //JavaRDD<String> lines = mSparkContext.textFile("s3n://dl4j-distribution/mnist_svmlight.txt",60000 / conf.getConf(0).getBatchSize());
    JavaRDD<String> lines = mSparkContext.textFile("/usr/local/hadoop_store/hdfs/datanode/mnist_svmlight.txt", 600); // / conf.getConf(0).getBatchSize());
    RecordReader svmLight = new SVMLightRecordReader();
    Configuration canovaConf = new Configuration();
    //number of features + label
    canovaConf.setInt(SVMLightRecordReader.NUM_ATTRIBUTES,784);
    svmLight.setConf(canovaConf);

    JavaRDD<DataSet> data = lines.map(new RecordReaderFunction(svmLight, 784, 10));
    MultiLayerNetwork network2 = master.fitDataSet(data);
    FileOutputStream fos  = new FileOutputStream("params.txt");
    DataOutputStream dos = new DataOutputStream(fos);
    Nd4j.write(dos, network2.params());
    dos.flush();
    dos.close();

    org.nd4j.linalg.dataset.api.iterator.DataSetIterator iter = new MnistDataSetIterator(1000,60000);
    Evaluation eval = new Evaluation(10);
    while(iter.hasNext()) {
        DataSet next = iter.next();
        eval.eval(next.getLabels(), network.output(next.getFeatureMatrix(), true));
    }

    System.out.println(eval.stats());
  }
}
