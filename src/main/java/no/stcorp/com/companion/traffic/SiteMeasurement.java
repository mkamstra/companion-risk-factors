package no.stcorp.com.companion;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.time.*;
import java.time.format.*;


public class SiteMeasurement implements Serializable {
  	private static final long serialVersionUID = 500L;

	private String mMeasurementSiteReference;
	private Instant mMeasurementTimeDefault;
	private List<MeasuredValue> mMeasuredValues;

	public SiteMeasurement(String pMeasurementSiteReference, String pMeasurementTimeDefault) {
		mMeasurementSiteReference = pMeasurementSiteReference;
		try {
			mMeasurementTimeDefault = Instant.parse(pMeasurementTimeDefault);
		} catch (DateTimeParseException ex) {
			System.err.println("Time of site measurement not correctly formatted");
			ex.printStackTrace();
			mMeasurementTimeDefault = Instant.now();
		}
		mMeasuredValues = new ArrayList<MeasuredValue>();
	}

	public String getMeasurementSiteReference() {
		return mMeasurementSiteReference;
	}

	public Instant getMeasurementTimeDefault() {
		return mMeasurementTimeDefault;
	}

	public void addMeasuredValue(int pIndex, String pName, double pValue) {
		MeasuredValue measuredValue = new MeasuredValue(pIndex, pName, pValue);
		mMeasuredValues.add(measuredValue);
	}

	@Override
	public String toString() {
	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
	    String measurementValuesString = "";
	    for (MeasuredValue mv : mMeasuredValues) {
	    	measurementValuesString += mv + " ";
	    }
		return mMeasurementSiteReference + " (" + formatter.format(mMeasurementTimeDefault) + "): " + measurementValuesString;
	}
}