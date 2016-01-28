package no.stcorp.com.companion.visualization;

import java.io.*;

import javax.swing.*;
import javax.swing.filechooser.*;

public class TimeSeriesFileSelector {
	
	public File[] selectFiles() {
		JFileChooser fileChooser = new JFileChooser();
		FileNameExtensionFilter filter = new FileNameExtensionFilter("BOS files (binary chart data)", "bos");
		fileChooser.setFileFilter(filter);
		fileChooser.setCurrentDirectory(new File(System.getProperty("user.dir") + "/src/test/resources/chartdata/"));
		fileChooser.setDialogTitle("Select the files to display (hold CTRL to select more than one file)");
		fileChooser.setMultiSelectionEnabled(true);
		int returnValue = fileChooser.showOpenDialog(null);
		File[] selectedFiles = null;
		if (returnValue == JFileChooser.APPROVE_OPTION) {
			selectedFiles = fileChooser.getSelectedFiles();
			System.out.println("Selected files: ");
			for (File file : selectedFiles) {
				System.out.println(file.getPath());
			}
		}
		return selectedFiles;
	}


}