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
import org.jfree.data.time.Hour;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.date.SerialDate;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class Visualiser extends ApplicationFrame {

    final TimeSeries temperatureSeries = new TimeSeries("Temperature (C)", Hour.class);
    final TimeSeries precipitationSeries = new TimeSeries("Precipitation (mm/h)", Hour.class);
    final TimeSeries windspeedSeries = new TimeSeries("Wind Speed (m/s)", Hour.class);


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
        final IntervalXYDataset dataPrecipitation = new TimeSeriesCollection(precipitationSeries);
        final XYItemRenderer rendererHistogram = new XYBarRenderer(0.20);
        rendererHistogram.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        final DateAxis domainAxis = new DateAxis("Time");
        domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
        final ValueAxis rangeAxisPrecipitation = new NumberAxis("Precipitation (mm/h)");
        final XYPlot plot = new XYPlot(dataPrecipitation, domainAxis, rangeAxisPrecipitation, rendererHistogram);
        // final double x = new Day(9, SerialDate.MARCH, 2002).getMiddleMillisecond();
        // final XYTextAnnotation annotation = new XYTextAnnotation("Hello!", x, 10000.0);
        // annotation.setFont(new Font("SansSerif", Font.PLAIN, 9));
        // plot.addAnnotation(annotation);

        final ValueAxis rangeAxisTemperature = new NumberAxis("Temperature (C)");
        plot.setRangeAxis(1, rangeAxisTemperature);

        // add a second dataset and renderer...
        final IntervalXYDataset dataTemperature = new TimeSeriesCollection(temperatureSeries);
        final XYItemRenderer rendererLine = new StandardXYItemRenderer();
        rendererLine.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        plot.setDataset(1, dataTemperature);
        plot.mapDatasetToRangeAxis(1, 1);
        plot.setRenderer(1, rendererLine);
        
        final ValueAxis rangeAxisWindspeed = new NumberAxis("Wind speed (m/s)");
        plot.setRangeAxis(2, rangeAxisWindspeed);

        // add a second dataset and renderer...
        final IntervalXYDataset dataWindspeed = new TimeSeriesCollection(windspeedSeries);
        final XYItemRenderer rendererLine2 = new StandardXYItemRenderer();
        rendererLine.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        plot.setDataset(2, dataWindspeed);
        plot.mapDatasetToRangeAxis(2, 2);
        plot.setRenderer(2, rendererLine2);

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

}