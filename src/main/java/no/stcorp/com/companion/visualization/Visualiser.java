package no.stcorp.com.companion.visualization;

import java.io.*;
import java.nio.file.*;

import java.awt.Font;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import java.time.*;
import java.util.Date;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickMarkPosition;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.time.*;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.date.SerialDate;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class Visualiser extends ApplicationFrame {
  	private static final long serialVersionUID = 900L;

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
     * Constructs a new demonstration application.
     *
     * @param title  the frame title.
     */
    public Visualiser(final String title) {
        super(title);
    }

    public void importDataSeries(SeriesType pSeriesType, String pFileName) {
    	try {
	    	Path path = Paths.get(pFileName);
	    	byte[] byteArray = Files.readAllBytes(path);
	    	ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(byteArray));
    		switch (pSeriesType) {
    			case TRAFFICSPEED:
    				trafficspeedSeries = (TimeSeries) in.readObject();
    				break;
    			case TEMPERATURE:
    				temperatureSeries = (TimeSeries) in.readObject();
    				break;
    			case PRECIPITATION:
    				precipitationSeries = (TimeSeries) in.readObject();
    				break;
    			case WINDSPEED:
    				windspeedSeries = (TimeSeries) in.readObject();
    				break;
    			default:
    				break;
	    	}
	    	in.close();
	    } catch (Exception ex) {
	    	ex.printStackTrace();
	    }
    }

	/**
	 * Create the chart after all data has been filled
	 */
    public void create(String pNdwId, String pStartDate, String pEndDate, boolean pSaveToFile) {
        final JFreeChart chart = createOverlaidChart(pNdwId, pStartDate, pEndDate, pSaveToFile);
        final ChartPanel panel = new ChartPanel(chart, true, true, true, true, true);
        panel.setPreferredSize(new java.awt.Dimension(800, 500));
        setContentPane(panel);
    }

    private void writeDataToFile(SeriesType pSeriesType, String pNdwId, String pStartDate, String pEndDate) {
		OutputStream fileOutput = null;
		ByteArrayOutputStream baos = null;
		ObjectOutput out = null;
    	try {
    		String fileName = "/tmp/" + pSeriesType + "_" + pNdwId + "_" + pStartDate + "_" + pEndDate + ".bos";
    		System.out.println("Writing chart data to file: " + fileName);
    		fileOutput = new FileOutputStream(fileName);
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

    /**
     * Creates an overlaid chart.
     *
     * @return The chart.
     */
    private JFreeChart createOverlaidChart(String pNdwId, String pStartDate, String pEndDate, boolean pSaveToFile) {
    	// Serialize data to file to be able to retrieve it later
    	if (pSaveToFile) {
	    	writeDataToFile(SeriesType.TRAFFICSPEED, pNdwId, pStartDate, pEndDate);
	    	writeDataToFile(SeriesType.TEMPERATURE, pNdwId, pStartDate, pEndDate);
	    	writeDataToFile(SeriesType.PRECIPITATION, pNdwId, pStartDate, pEndDate);
	    	writeDataToFile(SeriesType.WINDSPEED, pNdwId, pStartDate, pEndDate);
		}

        // create plot ...
        // final IntervalXYDataset data1 = createDataset1();
        final DateAxis domainAxis = new DateAxis("Time");
        domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
        final ValueAxis rangeAxisTrafficspeed = new NumberAxis("Traffic speed (km/h)");
        rangeAxisTrafficspeed.setLowerBound(trafficspeedSeries.getMinY());
        rangeAxisTrafficspeed.setUpperBound(1.5 * trafficspeedSeries.getMaxY());
        final IntervalXYDataset dataTrafficspeed = new TimeSeriesCollection(trafficspeedSeries);
        final XYItemRenderer rendererHistogramTraffic = new XYBarRenderer(0.20);
        rendererHistogramTraffic.setBaseToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        final XYItemRenderer rendererLineTraffic = new StandardXYItemRenderer();
        rendererLineTraffic.setBaseToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        XYPlot plot = new XYPlot(dataTrafficspeed, domainAxis, rangeAxisTrafficspeed, rendererHistogramTraffic);

        final ValueAxis rangeAxisPrecipitation = new NumberAxis("Precipitation (mm/h)");
        rangeAxisPrecipitation.setLowerBound(-2.5 * precipitationSeries.getMaxY());
        rangeAxisPrecipitation.setUpperBound(1.2 * precipitationSeries.getMaxY());
        plot.setRangeAxis(1, rangeAxisPrecipitation);
        final IntervalXYDataset dataPrecipitation = new TimeSeriesCollection(precipitationSeries);
        final XYItemRenderer rendererHistogramPrecipitation = new XYBarRenderer(0.20);
        rendererHistogramPrecipitation.setBaseToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        plot.setDataset(1, dataPrecipitation);
        plot.mapDatasetToRangeAxis(1, 1);
        plot.setRenderer(1, rendererHistogramPrecipitation);

        // final double x = new Day(9, SerialDate.MARCH, 2002).getMiddleMillisecond();
        // final XYTextAnnotation annotation = new XYTextAnnotation("Hello!", x, 10000.0);
        // annotation.setFont(new Font("SansSerif", Font.PLAIN, 9));
        // plot.addAnnotation(annotation);

        final ValueAxis rangeAxisTemperature = new NumberAxis("Temperature (C)");
        rangeAxisTemperature.setLowerBound(-temperatureSeries.getMaxY());
        rangeAxisTemperature.setUpperBound(1.2 * temperatureSeries.getMaxY());
        plot.setRangeAxis(2, rangeAxisTemperature);
        final IntervalXYDataset dataTemperature = new TimeSeriesCollection(temperatureSeries);
        final XYItemRenderer rendererLineTemperature = new StandardXYItemRenderer();
        rendererLineTemperature.setBaseToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        plot.setDataset(2, dataTemperature);
        plot.mapDatasetToRangeAxis(2, 2);
        plot.setRenderer(2, rendererLineTemperature);
        
        final ValueAxis rangeAxisWindspeed = new NumberAxis("Wind speed (m/s)");
        rangeAxisWindspeed.setLowerBound(-2.0 * windspeedSeries.getMaxY());
        rangeAxisWindspeed.setUpperBound(1.2 * windspeedSeries.getMaxY());
        plot.setRangeAxis(3, rangeAxisWindspeed);
        final IntervalXYDataset dataWindspeed = new TimeSeriesCollection(windspeedSeries);
        final XYItemRenderer rendererLineWindspeed = new StandardXYItemRenderer();
        rendererLineWindspeed.setBaseToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        plot.setDataset(3, dataWindspeed);
        plot.mapDatasetToRangeAxis(3, 3);
        plot.setRenderer(3, rendererLineWindspeed);

        plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);

        // return a new chart containing the overlaid plot...
        return new JFreeChart("Weather at measurement site " + pNdwId + " from " + pStartDate + " until " + pEndDate, JFreeChart.DEFAULT_TITLE_FONT, plot, true);

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
}