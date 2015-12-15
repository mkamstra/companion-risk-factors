import java.util.List;
import java.util.ArrayList;
import java.time.Instant;
import java.time.format.DateTimeParseException;

public class SiteMeasurement {
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
}