package com.gsihadoop.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class SP500List {
	private static String[] list;
	
	private SP500List(){
		list = new String[500];
	}

	public static String[] parseFile(Path filePath) {
		try {
			Scanner scanner = new Scanner(filePath);
			readFile(scanner);
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	private static void readFile(Scanner scanner) {
		List<String> tempList = new ArrayList<String>();
		while (scanner.hasNextLine()){
			String line = scanner.nextLine();
			tempList.add(line.trim());
		}
		list = tempList.toArray(new String[tempList.size()]);
	}

	public String[] getSP500List(){
		return list;
	}
}
