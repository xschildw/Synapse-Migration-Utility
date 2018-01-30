package org.sagebionetworks.migration.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;

public interface FileProvider {
	
	/**
	 * Get a file given a full path.
	 * @param path
	 * @return
	 */
	public File getFile(String path);

	/**
	 * Create an input stream for the given file.
	 * @param file
	 * @return
	 * @throws FileNotFoundException 
	 */
	public InputStream createInputStream(File file) throws FileNotFoundException;

}
