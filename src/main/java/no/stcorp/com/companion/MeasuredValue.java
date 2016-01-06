package no.stcorp.com.companion;

public class MeasuredValue {
	private int mIndex;
	private String mType;
	private double mValue;

	public MeasuredValue(int pIndex, String pType, double pValue) {
		mIndex = pIndex;
		mType = pType;
		mValue = pValue;
	}

	public int getIndex() {
		return mIndex;
	}
	
	public String getType() {
		return mType;
	}

	public double getValue() {
		return mValue;
	}
}