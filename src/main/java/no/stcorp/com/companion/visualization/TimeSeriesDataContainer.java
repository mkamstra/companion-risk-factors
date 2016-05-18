package no.stcorp.com.companion.visualization;

import java.io.*;
import java.nio.file.*;

import java.time.*;
import java.util.Date;
import java.util.List;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5SimpleWriter;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import no.stcorp.com.companion.util.Utils;
import org.jfree.data.time.*;

/**
 * Class holding the data that for example can be plotted
 */
public class TimeSeriesDataContainer {
	public enum SeriesType {
	  TRAFFICSPEED, 
	  TEMPERATURE, 
	  PRECIPITATION, 
	  WINDSPEED;
	}

	TimeSeries temperatureSeries = new TimeSeries("Temperature (C)");
	TimeSeries precipitationSeries = new TimeSeries("Precipitation (mm/h)");
	TimeSeries windspeedSeries = new TimeSeries("Wind Speed (m/s)");
	TimeSeries trafficspeedSeries = new TimeSeries("Traffic speed (km/h)");

	/**
	 * Read data from file for a specific time series
	 */
    public void importDataSeries(SeriesType pSeriesType, String pFileName) {
    	try {
	    	Path path = Paths.get(pFileName);
	    	byte[] byteArray = Files.readAllBytes(path);
	    	ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(byteArray));
            TimeSeries tmpSeries = (TimeSeries) in.readObject();
    		switch (pSeriesType) {
    			case TRAFFICSPEED:
    				trafficspeedSeries.addAndOrUpdate(tmpSeries) ;
    				break;
    			case TEMPERATURE:
                    temperatureSeries.addAndOrUpdate(tmpSeries);
    				break;
    			case PRECIPITATION:
                    precipitationSeries.addAndOrUpdate(tmpSeries);
    				break;
    			case WINDSPEED:
                    windspeedSeries.addAndOrUpdate(tmpSeries);
    				break;
    			default:
    				break;
	    	}
	    	in.close();
	    } catch (Exception ex) {
	    	ex.printStackTrace();
	    }
    }


	public void writeHDF5(IHDF5Writer writer, String pNdwId) {

		int[] trafficIntDims = {1, 3};  // default is a NaN entry
		double[][] trafficData = {{Double.NaN, Double.NaN, Double.NaN}};
		int[] temperatureIntDims = {1, 3};  // default is a NaN entry
		double[][] temperatureData = {{Double.NaN, Double.NaN, Double.NaN}};
		int[] precipitationIntDims = {1, 3};  // default is a NaN entry
		double[][] precipitationData = {{Double.NaN, Double.NaN, Double.NaN}};
		int[] windspeedIntDims = {1, 3};  // default is a NaN entry
		double[][] windspeedData = {{Double.NaN, Double.NaN, Double.NaN}};

		// TrafficSeries
		if (trafficspeedSeries.getItemCount() > 0) {
			trafficIntDims = new int[] {trafficspeedSeries.getItemCount(), 3}; // timeseries are columns

			List<double[]> dataList = Utils.convertTimeSeriesToList(trafficspeedSeries);
			trafficData = dataList.toArray(new double[trafficIntDims[0]][trafficIntDims[1]]);
		}

		// TemperatureSeries
		if (temperatureSeries.getItemCount() > 0) {
			temperatureIntDims = new int[] {temperatureSeries.getItemCount(), 3}; // timeseries are columns

			List<double[]> dataList = Utils.convertTimeSeriesToList(temperatureSeries);
			temperatureData = dataList.toArray(new double[temperatureIntDims[0]][temperatureIntDims[1]]);
		}

		// PrecipitationSeries
		if (precipitationSeries.getItemCount() > 0) {
			precipitationIntDims = new int[] {precipitationSeries.getItemCount(), 3}; // timeseries are columns

			List<double[]> dataList = Utils.convertTimeSeriesToList(precipitationSeries);
			precipitationData = dataList.toArray(new double[precipitationIntDims[0]][precipitationIntDims[1]]);
		}

		// WindspeedSeries
		if (windspeedSeries.getItemCount() > 0) {
			windspeedIntDims = new int[] {windspeedSeries.getItemCount(), 3}; // timeseries are columns

			List<double[]> dataList = Utils.convertTimeSeriesToList(windspeedSeries);
			windspeedData = dataList.toArray(new double[windspeedIntDims[0]][windspeedIntDims[1]]);
		}

		writer.writeDoubleMatrix(pNdwId + "/trafficspeed", trafficData);
		writer.string().setAttr(pNdwId + "/trafficspeed", "units", "timestamp_start, timestamp_end, km/h");
		writer.writeDoubleMatrix(pNdwId + "/temperature", temperatureData);
		writer.string().setAttr(pNdwId + "/temperature", "units", "timestamp_start, timestamp_end, C");
		writer.writeDoubleMatrix(pNdwId + "/precipitation", precipitationData);
		writer.string().setAttr(pNdwId + "/precipitation", "units", "timestamp_start, timestamp_end, mm/h");
		writer.writeDoubleMatrix(pNdwId + "/windspeed", windspeedData);
		writer.string().setAttr(pNdwId + "/windspeed", "units", "timestamp_start, timestamp_end, m/s");

	}

    /**
     * Write data to file (byte arrays)
     */
    public void writeDataToFile(String pPath, String pNdwId, String pStartDate, String pEndDate) {
		String pFilePath = pPath + "_" + pNdwId + "_" + pStartDate + "_" + pEndDate + ".bos";

		writeDataToFile(SeriesType.TRAFFICSPEED, pFilePath, pStartDate, pEndDate);
    	writeDataToFile(SeriesType.TEMPERATURE, pFilePath, pStartDate, pEndDate);
    	writeDataToFile(SeriesType.PRECIPITATION, pFilePath, pStartDate, pEndDate);
    	writeDataToFile(SeriesType.WINDSPEED, pFilePath, pStartDate, pEndDate);
    }

    private void writeDataToFile(SeriesType pSeriesType, String pFilePath, String pStartDate, String pEndDate) {
		OutputStream fileOutput = null;
		ByteArrayOutputStream baos = null;
		ObjectOutput out = null;
    	try {
    		System.out.println("Writing chart data to file: " + pFilePath);
    		fileOutput = new FileOutputStream(pFilePath);
    		baos = new ByteArrayOutputStream();
    		out = new ObjectOutputStream(baos);
    		switch (pSeriesType) {
    			case TRAFFICSPEED:
    				out.writeObject(trafficspeedSeries);
    				break;
    			case TEMPERATURE:
    				out.writeObject(temperatureSeries);
    				break;
    			case PRECIPITATION:
    				out.writeObject(precipitationSeries);
    				break;
    			case WINDSPEED:
    				out.writeObject(windspeedSeries);
    				break;
    			default:
    				break;
    		}
    		baos.writeTo(fileOutput);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	} finally {
    		try {
	    		out.close();
	    		baos.close();
	    		fileOutput.close();
	    	} catch (Exception ex) {
	    		System.err.println("Error closing output");
	    		ex.printStackTrace();
	    	}
    	}
    }

    public void addTemperatureRecord(Instant pTime, double pTemperature) {
    	Hour hour = new Hour(Date.from(pTime));
    	if (temperatureSeries.getDataItem(hour) != null) {
    		System.err.println("Trying to add temperature at hour " + hour + " but a value for that hour exists already");
    	} else {
    		temperatureSeries.add(hour, pTemperature);
    	}
    }

    public void addPrecipitationRecord(Instant pTime, double pPrecipitation) {
    	Hour hour = new Hour(Date.from(pTime));
    	if (precipitationSeries.getDataItem(hour) != null) {
    		System.err.println("Trying to add precipitation at hour " + hour + " but a value for that hour exists already");
    	} else {
    		precipitationSeries.add(hour, pPrecipitation);
    	}
    }

    public void addWindspeedRecord(Instant pTime, double pWindspeed) {
    	Hour hour = new Hour(Date.from(pTime));
    	if (windspeedSeries.getDataItem(hour) != null) {
    		System.err.println("Trying to add wind speed at hour " + hour + " but a value for that hour exists already");
    	} else {
    		windspeedSeries.add(hour, pWindspeed);
    	}
    }

    public void addTrafficspeedRecord(Instant pTime, double pTrafficspeed) {
    	Minute minute = new Minute(Date.from(pTime));
    	if (trafficspeedSeries.getDataItem(minute) != null) {
    		System.err.println("Trying to add traffic speed at minute " + minute + " but a value for that minute exists already");
    	} else {
    		trafficspeedSeries.add(minute, pTrafficspeed);
    	}
    }
  
  	public TimeSeries getTrafficspeedSeries() {
  		return trafficspeedSeries;
  	}

  	public TimeSeries getTemperatureSeries() {
  		return temperatureSeries;
  	}

  	public TimeSeries getPrecipitationSeries() {
  		return precipitationSeries;
  	}

  	public TimeSeries getWindspeedSeries() {
  		return windspeedSeries;
  	}
}