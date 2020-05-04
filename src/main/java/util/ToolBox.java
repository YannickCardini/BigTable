package main.java.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import io.netty.handler.codec.http.multipart.FileUpload;

public class ToolBox {
	
	public static final String SEPARATOR = FileSystems.getDefault().getSeparator();
	public static final String DEFAULT_PATH = System.getProperty("user.dir");
	
	// create file into ressources folder (ex : "toto/toto.txt")
	public static File getFileIntoRessources(String name) throws IOException {
		return new File(getProjectDirectoryPath() + SEPARATOR + "ressources" + SEPARATOR +  name);
	}
	
	//get file path by name like "toto.txt"
	public static String getFileByName(File root, String name) {	
		File[] files = root.listFiles();		
		if(files != null) {
			for (File f : files) {
	            if(f.isDirectory()) {   
	                String path = getFileByName(f, name);
	                if (path == null) {
	                    continue;
	                }
	                else {
	                    return path;
	                }
	            }
	        }
		} else {
			System.out.println("[ERROR] - getFileByName - Le dossier entré est introuvable");
			return null;
		}
		return null;
	}
	
	//get the current directory project path
	public static String getProjectDirectoryPath() {
		return System.getProperty("user.dir");
	}
	
}
