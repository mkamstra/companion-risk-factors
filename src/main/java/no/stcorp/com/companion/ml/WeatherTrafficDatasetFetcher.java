package no.stcorp.com.companion.ml;

import org.deeplearning4j.datasets.iterator.DataSetFetcher;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.util.FeatureUtil;
import org.nd4j.linalg.util.ArrayUtil;

import java.util.*;

/**
 * A class for creation of matrices for the RNN
 */
public class WeatherTrafficDatasetFetcher implements DataSetFetcher {
	
	private static final long serialVersionUID = UUID.randomUUID().getMostSignificantBits();

  protected int mCursor = 0;
  protected int mNumOutcomes = -1;
  protected int mInputColumns = -1;
  protected DataSet mInitialDataSet = null;
  protected DataSet mCurrentDataSet = null;
  protected int mTotalExamples = -1;

  public WeatherTrafficDatasetFetcher() {
		System.out.println("Obtaining relevant data files");
		obtainDataFiles();
  }

  /**
   * Obtain the relevant data files
   */
  private void obtainDataFiles() {
  	// TODO: Implement: obtain relevant files and get relevant information from them
  	mInitialDataSet = null;
  }

 	/**
	 * Fetches the next dataset. You need to call this
	 * to getFromOrigin a new dataset, otherwise {@link #next()}
	 * just returns the last data applyTransformToDestination fetch
	 *
	 * @param numExamples the number of examples to fetch
	 */  @Override
  public void fetch(int numExamples) {
  	initializeCurrFromList(mInitialDataSet.get(ArrayUtil.range(mCursor, mCursor + numExamples)).asList());
  	mCursor += numExamples;
  }

  /**
   * Initializes this data transform fetcher from the passed in datasets
   *
   * @param examples the examples to use
   */
  protected void initializeCurrFromList(List<DataSet> examples) {

    if (examples.isEmpty())
      System.err.println("Warning: empty dataset from the fetcher");

    INDArray inputs = createInputMatrix(examples.size());
    INDArray labels = createOutputMatrix(examples.size());
    for (int i = 0; i < examples.size(); i++) {
      inputs.putRow(i, examples.get(i).getFeatureMatrix());
      labels.putRow(i, examples.get(i).getLabels());
    }
    mCurrentDataSet = new DataSet(inputs, labels);
  }

  /**
   * Creates a feature vector
   *
   * @param numRows the number of examples
   * @return a feature vector
   */
  protected INDArray createInputMatrix(int numRows) {
    return Nd4j.create(numRows, mInputColumns);
  }

  /**
   * Creates an output label matrix
   *
   * @param outcomeLabel the outcome label to use
   * @return a binary vector where 1 is transformed to the index specified by outcomeLabel
   */
  protected INDArray createOutputVector(int outcomeLabel) {
    return FeatureUtil.toOutcomeVector(outcomeLabel, mNumOutcomes);
  }

  protected INDArray createOutputMatrix(int numRows) {
    return Nd4j.create(numRows, mNumOutcomes);
  }

  @Override
  public boolean hasMore() {
    return mCursor < mTotalExamples;
  }

  @Override
  public DataSet next() {
    return mCurrentDataSet;
  }

  @Override
  public int totalOutcomes() {
    return mNumOutcomes;
  }

  @Override
  public int inputColumns() {
    return mInputColumns;
  }

  @Override
  public int totalExamples() {
    return mTotalExamples;
  }

  @Override
  public void reset() {
    mCursor = 0;
  }

  @Override
  public int cursor() {
    return mCursor;
  }

	
}