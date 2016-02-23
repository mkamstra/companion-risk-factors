package no.stcorp.com.companion.ml;

import org.deeplearning4j.datasets.iterator.DataSetIterator;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.DataSetPreProcessor;
import org.nd4j.linalg.factory.Nd4j;

import org.apache.spark.api.java.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

/** 
 * A very simple DataSetIterator for use in the GravesLSTMCharModellingExample.
 * Given a text file and a few options, generate feature vectors and labels for training,
 * where we want to predict the next character in the sequence.<br>
 * This is done by randomly choosing a position in the text file to start the sequence and
 * (optionally) scanning backwards to a new line (to ensure we don't start half way through a word
 * for example).<br>
 * Feature vectors and labels are both one-hot vectors of same length
 * @author Alex Black
 */
public class CharacterIterator implements DataSetIterator {
	private static final long serialVersionUID = UUID.randomUUID().getMostSignificantBits();;
	private static final int MAX_SCAN_LENGTH = 200; 
	private char[] mValidCharacters;
	private Map<Character,Integer> mCharToIdxMap;
	private char[] mFileCharacters;
	private int mExampleLength;
	private int mMiniBatchSize;
	private int mNumExamplesToFetch;
	private int mExamplesSoFar = 0;
	private Random rng;
	private final int mNumCharacters;
	private final boolean mAlwaysStartAtNewLine;
	
	public CharacterIterator(JavaSparkContext pSparkContext, String path, int miniBatchSize, int exampleSize, int numExamplesToFetch ) throws IOException {
		this(pSparkContext, path, Charset.defaultCharset(), miniBatchSize, exampleSize, numExamplesToFetch, getDefaultCharacterSet(), new Random(), true);
	}
	
	/**
	 * @param textFilePath Path to text file to use for generating samples
	 * @param textFileEncoding Encoding of the text file. Can try Charset.defaultCharset()
	 * @param miniBatchSize Number of examples per mini-batch
	 * @param exampleLength Number of characters in each input/output vector
	 * @param numExamplesToFetch Total number of examples to fetch (must be multiple of miniBatchSize). Used in hasNext() etc methods
	 * @param validCharacters Character array of valid characters. Characters not present in this array will be removed
	 * @param rng Random number generator, for repeatability if required
	 * @param alwaysStartAtNewLine if true, scan backwards until we find a new line character (up to MAX_SCAN_LENGTH in case
	 *  of no new line characters, to avoid scanning entire file)
	 * @throws IOException If text file cannot  be loaded
	 */
	public CharacterIterator(JavaSparkContext pSparkContext, String textFilePath, Charset textFileEncoding, int miniBatchSize, int exampleLength,
			int numExamplesToFetch, char[] validCharacters, Random rng, boolean alwaysStartAtNewLine ) throws IOException {
		if( !new File(textFilePath).exists()) throw new IOException("Could not access file (does not exist): " + textFilePath);
		if(numExamplesToFetch % miniBatchSize != 0 ) throw new IllegalArgumentException("numExamplesToFetch must be a multiple of miniBatchSize");
		if( miniBatchSize <= 0 ) throw new IllegalArgumentException("Invalid miniBatchSize (must be >0)");
		this.mValidCharacters = validCharacters;
		this.mExampleLength = exampleLength;
		this.mMiniBatchSize = miniBatchSize;
		this.mNumExamplesToFetch = numExamplesToFetch;
		this.rng = rng;
		this.mAlwaysStartAtNewLine = alwaysStartAtNewLine;
		
		// Store valid characters in a map for later use in vectorization
		mCharToIdxMap = new HashMap<>();
		for( int i=0; i<validCharacters.length; i++ ) 
			mCharToIdxMap.put(validCharacters[i], i);

		mNumCharacters = mValidCharacters.length;
		
		//Load file and convert contents to a char[] 
		boolean newLineValid = mCharToIdxMap.containsKey('\n');
    JavaRDD<String> linesRDD = pSparkContext.textFile(textFilePath).cache(); // textFile should decompress gzip automatically
    List<String> lines = linesRDD.collect(); 
		// List<String> lines = Files.readAllLines(new File(textFilePath).toPath(),textFileEncoding);
		int maxSize = lines.size();	//add lines.size() to account for newline characters at end of each line which are also stored
		for( String s : lines ) 
			maxSize += s.length();

		char[] characters = new char[maxSize];
		int currIdx = 0;
		for (String s : lines) {
			char[] thisLine = s.toCharArray();
			for (char aThisLine : thisLine) {
				if (!mCharToIdxMap.containsKey(aThisLine)) continue; // Check if charater is allowed. If not, ignore character
				characters[currIdx++] = aThisLine;
			}
			if (newLineValid) characters[currIdx++] = '\n'; // Newline is also stored in character array
		}
		
		if( currIdx == characters.length ){
			mFileCharacters = characters;
		} else {
			mFileCharacters = Arrays.copyOfRange(characters, 0, currIdx);
		}
		if( mExampleLength >= mFileCharacters.length ) throw new IllegalArgumentException("exampleLength=" + exampleLength
				+ " cannot exceed number of valid characters in file (" + mFileCharacters.length +")");
		
		int nRemoved = maxSize - mFileCharacters.length;
		System.out.println("Loaded and converted file: " + mFileCharacters.length + " valid characters of "
		+ maxSize + " total characters (" + nRemoved + " removed)");
	}
	
	/** A minimal character set, with a-z, A-Z, 0-9 and common punctuation etc */
	public static char[] getMinimalCharacterSet(){
		List<Character> validChars = new LinkedList<>();
		for(char c='a'; c<='z'; c++) validChars.add(c);
		for(char c='A'; c<='Z'; c++) validChars.add(c);
		for(char c='0'; c<='9'; c++) validChars.add(c);
		char[] temp = {'!', '&', '(', ')', '?', '-', '\'', '"', ',', '.', ':', ';', ' ', '\n', '\t'};
		for( char c : temp ) validChars.add(c);
		char[] out = new char[validChars.size()];
		int i=0;
		for( Character c : validChars ) out[i++] = c;
		return out;
	}
	
	/** As per getMinimalCharacterSet(), but with a few extra characters */
	public static char[] getDefaultCharacterSet(){
		List<Character> validChars = new LinkedList<>();
		for(char c : getMinimalCharacterSet() ) validChars.add(c);
		char[] additionalChars = {'@', '#', '$', '%', '^', '*', '{', '}', '[', ']', '/', '+', '_',
				'\\', '|', '<', '>'};
		for( char c : additionalChars ) validChars.add(c);
		char[] out = new char[validChars.size()];
		int i=0;
		for( Character c : validChars ) out[i++] = c;
		return out;
	}
	
	public char convertIndexToCharacter( int idx ){
		return mValidCharacters[idx];
	}
	
	public int convertCharacterToIndex( char c ){
		return mCharToIdxMap.get(c);
	}
	
	public char getRandomCharacter(){
		return mValidCharacters[(int) (rng.nextDouble()*mValidCharacters.length)];
	}

	public boolean hasNext() {
		return mExamplesSoFar + mMiniBatchSize <= mNumExamplesToFetch;
	}

	public DataSet next() {
		return next(mMiniBatchSize);
	}

	public DataSet next(int num) {
		if (mExamplesSoFar + num > mNumExamplesToFetch) throw new NoSuchElementException();
		//Allocate space:
		INDArray input = Nd4j.zeros(num, mNumCharacters, mExampleLength);
		INDArray labels = Nd4j.zeros(num, mNumCharacters, mExampleLength);
		
		int maxStartIdx = mFileCharacters.length - mExampleLength;
		
		//Randomly select a subset of the file. No attempt is made to avoid overlapping subsets
		// of the file in the same minibatch
		for( int i=0; i<num; i++ ){
			int startIdx = (int) (rng.nextDouble() * maxStartIdx);
			int endIdx = startIdx + mExampleLength;
			int scanLength = 0;
			if (mAlwaysStartAtNewLine){
				while (startIdx >= 1 && mFileCharacters[startIdx-1] != '\n' && scanLength++ < MAX_SCAN_LENGTH ) {
					startIdx--;
					endIdx--;
				}
			}
			
			int currCharIdx = mCharToIdxMap.get(mFileCharacters[startIdx]);	//Current input
			int c=0;
			for( int j=startIdx+1; j<=endIdx; j++, c++ ){
				int nextCharIdx = mCharToIdxMap.get(mFileCharacters[j]);		//Next character to predict
				input.putScalar(new int[]{i,currCharIdx,c}, 1.0);
				labels.putScalar(new int[]{i,nextCharIdx,c}, 1.0);
				currCharIdx = nextCharIdx;
			}
		}
		
		mExamplesSoFar += num;
		return new DataSet(input,labels);
	}

	public int totalExamples() {
		return mNumExamplesToFetch;
	}

	public int inputColumns() {
		return mNumCharacters;
	}

	public int totalOutcomes() {
		return mNumCharacters;
	}

	public void reset() {
		mExamplesSoFar = 0;
	}

	public int batch() {
		return mMiniBatchSize;
	}

	public int cursor() {
		return mExamplesSoFar;
	}

	public int numExamples() {
		return mNumExamplesToFetch;
	}

	public void setPreProcessor(DataSetPreProcessor preProcessor) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public List<String> getLabels() {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}