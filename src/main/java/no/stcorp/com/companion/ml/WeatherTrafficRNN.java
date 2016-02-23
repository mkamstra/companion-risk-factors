package no.stcorp.com.companion.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.nd4j.linalg.dataset.DataSet;

import java.io.*;

import java.util.UUID;

public class WeatherTrafficRNN implements Serializable {

  private static final long serialVersionUID = UUID.randomUUID().getMostSignificantBits();

  private static JavaSparkContext mSparkContext;

  /**
   * Constructor
   * @param pSparkContext The Spark context needed for example to run the machine learning algorithm
   */
  public WeatherTrafficRNN(SparkConf pSparkConf) {
    mSparkContext = new JavaSparkContext(pSparkConf); // JavaSparkContext object tells Spark how to access a cluster
  }

  public void run() {
  	WeatherTrafficDatasetIterator iter = getWeatherTrafficDatasetIterator();
  	if (iter != null) {
  		while (iter.hasNext()) {
  			DataSet dataSet = iter.next();
  		}
  	}
  }

  /**
   * Determine the folders and/or files that need to be obtained for training the NN and create a DataSetIterator
   * on top of it
   */
  private WeatherTrafficDatasetIterator getWeatherTrafficDatasetIterator() {
  	try {
  		System.out.println("Obtaining the relevant data files");
  		System.out.println("Creating a DataSetIterator on the relevant data files");
  		int batchSize = 100;
  		int numExamples = 200;
  		WeatherTrafficDatasetFetcher fetcher = new WeatherTrafficDatasetFetcher();
  		return new WeatherTrafficDatasetIterator(batchSize, numExamples, fetcher);
  	} catch (Exception ex) {
  		System.err.println("Something went wrong while creating a DataSetIterator from the relevant data: " + ex.getMessage());
  		ex.printStackTrace();
  	}
  	return null;
  }

}