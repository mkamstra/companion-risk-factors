package no.stcorp.com.companion.ml;

import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.datasets.iterator.DataSetFetcher;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.DataSetPreProcessor;

import java.util.*;

/**
 * A weather traffic dataset consists of time series of weather and traffic data for measurement sites. 
 */
public class WeatherTrafficDatasetIterator implements DataSetIterator {
	private static final long serialVersionUID = UUID.randomUUID().getMostSignificantBits();

	private int mExampleLength = -1;
	private int mBatchSize = -1;
	private int mNumExamplesToFetch = -1;
	private DataSetPreProcessor mPreProcessor = null;
	private DataSetFetcher mFetcher = null;

	public WeatherTrafficDatasetIterator(int batchSize, int numExamples, DataSetFetcher fetcher) {
		mBatchSize = batchSize;
		if (numExamples < 0)
			numExamples = fetcher.totalExamples();

		mNumExamplesToFetch = numExamples;
		mFetcher = fetcher;
	}

	@Override
	public boolean hasNext() {
		return mFetcher.hasMore() && mFetcher.cursor() < mNumExamplesToFetch;
	}

	@Override
	public DataSet next() {
		mFetcher.fetch(mBatchSize);
		return mFetcher.next();
	}

	@Override
	public DataSet next(int num) {
		mFetcher.fetch(num);
		DataSet nextDataSet = mFetcher.next();
		if (mPreProcessor != null)
			mPreProcessor.preProcess(nextDataSet);

		return nextDataSet;
	}	

	@Override
	public int numExamples() {
		return mNumExamplesToFetch;
	}

	@Override
	public int totalExamples() {
		return mFetcher.totalExamples();
	}

	@Override
	public int cursor() {
		return mFetcher.cursor();
	}

	@Override
	public int batch() {
		return mBatchSize;
	}

	@Override
	public void reset() {
		mFetcher.reset();
	}

	/**
	 * The number of labels for the dataset
	 */
	@Override
	public int totalOutcomes() {
		return mFetcher.totalOutcomes();
	}

	/**
	 * The number of input columsn for the dataset
	 */
	@Override
	public int inputColumns() {
		return mFetcher.inputColumns();
	}

	@Override
	public List<String> getLabels() {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public void setPreProcessor(DataSetPreProcessor preProcessor) {
		mPreProcessor = preProcessor;
	}

}