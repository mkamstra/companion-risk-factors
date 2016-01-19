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

    final TimeSeries temperatureSeries = new TimeSeries("Temperature", Hour.class);
    final TimeSeries precipitationSeries = new TimeSeries("Precipitation", Hour.class);


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
        final XYItemRenderer rendererPrecipitation = new XYBarRenderer(0.20);
        rendererPrecipitation.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        final DateAxis domainAxis = new DateAxis("Time");
        domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
        final ValueAxis rangeAxisPrecipitation = new NumberAxis("Precipitation (mm/h)");
        final XYPlot plot = new XYPlot(dataPrecipitation, domainAxis, rangeAxisPrecipitation, rendererPrecipitation);
        // final double x = new Day(9, SerialDate.MARCH, 2002).getMiddleMillisecond();
        // final XYTextAnnotation annotation = new XYTextAnnotation("Hello!", x, 10000.0);
        // annotation.setFont(new Font("SansSerif", Font.PLAIN, 9));
        // plot.addAnnotation(annotation);

        final ValueAxis rangeAxisTemperature = new NumberAxis("Temperature (C)");
        plot.setRangeAxis(1, rangeAxisTemperature);

        // add a second dataset and renderer...
        // final XYDataset data2 = createDataset2();
        final IntervalXYDataset dataTemperature = new TimeSeriesCollection(temperatureSeries);
        final XYItemRenderer rendererTemperature = new StandardXYItemRenderer();
        rendererTemperature.setToolTipGenerator(
            new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("yyyy-MM-dd"), new DecimalFormat("0.00")
            )
        );
        plot.setDataset(1, dataTemperature);
        plot.mapDatasetToRangeAxis(1, 1);
        plot.setRenderer(1, rendererTemperature);
        
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

    // /**
    //  * Creates a sample dataset.
    //  *
    //  * @return The dataset.
    //  */
    // private IntervalXYDataset createDataset1() {

    //     // create dataset 1...
    //     final TimeSeries series1 = new TimeSeries("Series 1", Day.class);
    //     series1.add(new Day(1, SerialDate.MARCH, 2002), 12353.3);
    //     series1.add(new Day(2, SerialDate.MARCH, 2002), 13734.4);
    //     series1.add(new Day(3, SerialDate.MARCH, 2002), 14525.3);
    //     series1.add(new Day(4, SerialDate.MARCH, 2002), 13984.3);
    //     series1.add(new Day(5, SerialDate.MARCH, 2002), 12999.4);
    //     series1.add(new Day(6, SerialDate.MARCH, 2002), 14274.3);
    //     series1.add(new Day(7, SerialDate.MARCH, 2002), 15943.5);
    //     series1.add(new Day(8, SerialDate.MARCH, 2002), 14845.3);
    //     series1.add(new Day(9, SerialDate.MARCH, 2002), 14645.4);
    //     series1.add(new Day(10, SerialDate.MARCH, 2002), 16234.6);
    //     series1.add(new Day(11, SerialDate.MARCH, 2002), 17232.3);
    //     series1.add(new Day(12, SerialDate.MARCH, 2002), 14232.2);
    //     series1.add(new Day(13, SerialDate.MARCH, 2002), 13102.2);
    //     series1.add(new Day(14, SerialDate.MARCH, 2002), 14230.2);
    //     series1.add(new Day(15, SerialDate.MARCH, 2002), 11235.2);

    //     return new TimeSeriesCollection(series1);

    // }

    // /**
    //  * Creates a sample dataset.
    //  *
    //  * @return The dataset.
    //  */
    // private XYDataset createDataset2() {

    //     // create dataset 2...
    //     final TimeSeries series2 = new TimeSeries("Series 2", Day.class);

    //     series2.add(new Day(3, SerialDate.MARCH, 2002), 50.0);
    //     series2.add(new Day(4, SerialDate.MARCH, 2002), 53.1);
    //     series2.add(new Day(5, SerialDate.MARCH, 2002), 52.3);
    //     series2.add(new Day(6, SerialDate.MARCH, 2002), 30.1);
    //     series2.add(new Day(7, SerialDate.MARCH, 2002), 42.9);
    //     series2.add(new Day(8, SerialDate.MARCH, 2002), 60.1);
    //     series2.add(new Day(9, SerialDate.MARCH, 2002), 63.0);
    //     series2.add(new Day(10, SerialDate.MARCH, 2002), 55.0);
    //     series2.add(new Day(11, SerialDate.MARCH, 2002), 34.0);
    //     series2.add(new Day(12, SerialDate.MARCH, 2002), 33.1);
    //     series2.add(new Day(13, SerialDate.MARCH, 2002), 39.1);
    //     series2.add(new Day(14, SerialDate.MARCH, 2002), 46.0);
    //     series2.add(new Day(15, SerialDate.MARCH, 2002), 53.1);
    //     series2.add(new Day(16, SerialDate.MARCH, 2002), 58.0);

    //     final TimeSeriesCollection tsc = new TimeSeriesCollection(series2);
    //     return tsc;

    // }

}