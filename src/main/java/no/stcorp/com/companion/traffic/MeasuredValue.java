package no.stcorp.com.companion;

import java.io.Serializable;

public class MeasuredValue implements Serializable {

 	private static final long serialVersionUID = 400L;

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

	@Override
	public String toString() {
		return mIndex + " (" + mType + "): " + mValue;
	}
}