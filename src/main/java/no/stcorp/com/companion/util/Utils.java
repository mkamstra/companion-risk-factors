package no.stcorp.com.companion.util;

import java.io.*;

import java.nio.file.*;

import java.util.Scanner;

/**
 * A container for some generally applicable methods
 */ 
public class Utils {
	
  public static void printFileDetailsForFolder(Path pFolder) {
    System.out.println("============== Files in folder: " + pFolder.toString() + " ================");
    try {
      Files.walk(pFolder).forEach(filePath -> {
        if (Files.isRegularFile(filePath)) {
          try {
            System.out.println(filePath + " (" + Files.size(filePath) + ")");
          } catch (AccessDeniedException e) {
            System.out.println("Problem accessing file " + filePath);
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    } catch (AccessDeniedException ex) {
      System.out.println("Problem accessing file");
      ex.printStackTrace();
    } catch (IOException ex) {
      ex.printStackTrace();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void waitForUserInput() {
    Scanner scan = new Scanner(System.in);
    System.out.println("Continue by pressing any key followed by ENTER");
    try {
      scan.nextInt();
    } catch (Exception ex) {
      // Not important to write anything
    }
  }


}