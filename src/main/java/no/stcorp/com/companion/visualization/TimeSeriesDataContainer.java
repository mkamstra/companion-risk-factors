package no.stcorp.com.companion.visualization;

import java.io.*;
import java.nio.file.*;

import java.time.*;
import java.util.Date;
import java.util.List;

import no.stcorp.com.companion.util.Utils;
import org.jfree.data.time.*;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;

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


	public void writeHDF5(String path, String pNdwId, String pStartDate, String pEndDate) {
		String fileName = path + "_" + pNdwId + "_" + pStartDate + "_" + pEndDate + ".hdf";

		// TrafficSeries
		int fileId = -1;
		int datasetId = -1;
		int dcplId = -1;
		int spaceId = -1;
		int typeId = -1;
		int[] intdims = {trafficspeedSeries.getItemCount(), 2}; // timeseries are columns
		long[] longdims = {trafficspeedSeries.getItemCount(), 2};

		// long[] maxdims = {HDF5Constants.H5F_UNLIMITED, 2};

		List<double[]> dataList = Utils.convertTimeSeriesToList(trafficspeedSeries);
		double[][] data = dataList.toArray(new double[intdims[0]][intdims[1]]);

		try {
			fileId = H5.H5Fcreate(fileName, HDF5Constants.H5F_ACC_TRUNC, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);

			dcplId = H5.H5Pcreate(HDF5Constants.H5P_DATASET_CREATE);

			spaceId = H5.H5Screate_simple(2, longdims, null);

			typeId = H5.H5Tarray_create(HDF5Constants.H5T_INTEL_F64, 2, intdims, null);

		} catch (Exception e){
			e.printStackTrace();
		}

		try {

			if ((fileId >= 0) && (dcplId >= 0)) {
				datasetId = H5.H5Dcreate(fileId, "trafficspeed", typeId, spaceId, dcplId);
			}

			if (datasetId >= 0) {
				H5.H5Dwrite(datasetId, typeId, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, data);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				H5.H5Dclose(datasetId);
				H5.H5Tclose(typeId);
				H5.H5Sclose(spaceId);
				H5.H5Pclose(dcplId);
				H5.H5Fclose(fileId);
			} catch (Exception f) {
				f.printStackTrace();
			}
		}
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