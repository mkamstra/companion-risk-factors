import java.io.*;
import java.util.concurrent.ThreadLocalRandom;

public class MLInputGenerator {

	public static void main(String[] args) {
		File file = null;
		try{
			file = new File("mysvm.txt");
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			int nrOfRows = 10000;
			int nrOfFeatures = 3;
			for (int row = 0; row < nrOfRows; row++) {
				// Feature 1 represents temperature. Normal distrution around 10
				double temperature = 12.0 + 10.0 * ThreadLocalRandom.current().nextGaussian() + 273.0;
				// Feature 2 represents precipication. First draw if there is precipitation, then quantity
				boolean precipitationTrue = ThreadLocalRandom.current().nextDouble() > 0.75 ? true : false;
				double precipitation = 0.0;
				if (precipitationTrue) {
					precipitation = Math.max(0.0, 4.0 + 3.0 * ThreadLocalRandom.current().nextGaussian());
				}
				// Feature 3 represents wind strength.
				double wind = Math.max(0.0, 5.0 + 5.0 * ThreadLocalRandom.current().nextGaussian());

				// Determine label with some random behaviour to test the ML algorithm
				double labelValue = 0.0;
				if (precipitation > 0) {
					labelValue = Math.min(9.0, Math.max(0.0, 3.0 + (2.0 + precipitation / 2.0) * ThreadLocalRandom.current().nextGaussian())); // Rain reduction
				} else {
					labelValue = Math.max(0.0, 1.5 * ThreadLocalRandom.current().nextGaussian());
				}
				//System.out.println("*********");
				//System.out.println("Label value after precipitation: " + labelValue);
				labelValue = labelValue * (1 + Math.max(0.0, 0.5 * ThreadLocalRandom.current().nextGaussian() * (Math.abs(temperature - 285.0) / 10.0))); // Temperature reduction
				//System.out.println("Label value after temperature: " + labelValue);
				labelValue = labelValue * (1 + Math.max(0.0, 0.5 * ThreadLocalRandom.current().nextGaussian() * wind / 5.0)); // Wind reduction
				long label = Math.min(9, Math.max(0, Math.round(labelValue))); // Excluding upper limit, 0 = 90 - 100, 1 = 80 - 90, 2 = 70 - 80, etc... km/h
				System.out.println("Precipitation: " + precipitation + ", temperature: " + temperature + ", wind: " + wind + ": label = " + label);
				bw.write(label + " ");

				bw.write(1 + ":" + temperature + " ");
				bw.write(2 + ":" + precipitation + " ");
				bw.write(3 + ":" + wind + " ");
				bw.write("\n");
			}
			bw.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
}