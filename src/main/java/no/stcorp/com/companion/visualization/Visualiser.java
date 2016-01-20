package no.stcorp.com.companion.visualization;

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

    final TimeSeries temperatureSeries = new TimeSeries("Temperature (C)", Hour.class);
    final TimeSeries precipitationSeries = new TimeSeries("Precipitation (mm/h)", Hour.class);
    final TimeSeries windspeedSeries = new TimeSeries("Wind Speed (m/s)", Hour.class);
    final TimeSeries trafficspeedSeries = new TimeSeries("Traffic speed (km/h)", Minute.class);


    /**
     * Constructs a new demonstration application.
     *
     * @param title  the frame title.
     */
    public Visualiser(final String title) {
        super(title);
    }

	/**
	 * Create the chart after all data has been filled
	 */
    public void create(String pNdwId, String pStartDate, String pEndDate) {
        final JFreeChart chart = createOverlaidChart(pNdwId, pStartDate, pEndDate);
        final ChartPanel panel = new ChartPanel(chart, true, true, true, true, true);
        panel.setPreferredSize(new java.awt.Dimension(800, 500));
        setContentPane(panel);
    }

    /**
     * Creates an overlaid chart.
     *
     * @return The chart.
     */
    private JFreeChart createOverlaidChart(String pNdwId, String pStartDate, String pEndDate) {

        // create plot ...
        // final IntervalXYDataset data1 = createDataset1();
        final DateAxis domainAxis = new DateAxis("Time");
        domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
        final ValueAxis rangeAxisTrafficspeed = new NumberAxis("Traffic speed (km/h)");
        final IntervalXYDataset dataTrafficspeed = new TimeSeriesCollection(trafficspeedSeries);
        final XYItemRenderer rendererHistogramTraffic = new XYBarRenderer(0.20);
        rendererHistogramTraffic.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        final XYItemRenderer rendererLineTraffic = new StandardXYItemRenderer();
        rendererLineTraffic.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        XYPlot plot = new XYPlot(dataTrafficspeed, domainAxis, rangeAxisTrafficspeed, rendererHistogramTraffic);


        final ValueAxis rangeAxisPrecipitation = new NumberAxis("Precipitation (mm/h)");
        plot.setRangeAxis(1, rangeAxisPrecipitation);
        final IntervalXYDataset dataPrecipitation = new TimeSeriesCollection(precipitationSeries);
        final XYItemRenderer rendererHistogramPrecipitation = new XYBarRenderer(0.20);
        rendererHistogramPrecipitation.setToolTipGenerator(
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
        plot.setRangeAxis(2, rangeAxisTemperature);
        final IntervalXYDataset dataTemperature = new TimeSeriesCollection(temperatureSeries);
        final XYItemRenderer rendererLineTemperature = new StandardXYItemRenderer();
        rendererLineTemperature.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        plot.setDataset(2, dataTemperature);
        plot.mapDatasetToRangeAxis(2, 2);
        plot.setRenderer(2, rendererLineTemperature);
        
        final ValueAxis rangeAxisWindspeed = new NumberAxis("Wind speed (m/s)");
        plot.setRangeAxis(3, rangeAxisWindspeed);
        final IntervalXYDataset dataWindspeed = new TimeSeriesCollection(windspeedSeries);
        final XYItemRenderer rendererLineWindspeed = new StandardXYItemRenderer();
        rendererLineWindspeed.setToolTipGenerator(
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