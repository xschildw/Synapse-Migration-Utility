package org.sagebionetworks.migration.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class FileProviderImp implements FileProvider {

	@Override
	public File getFile(String path) {
		return new File(path);
	}

	@Override
	public InputStream createInputStream(File file) throws FileNotFoundException {
		return new FileInputStream(file);
	}

}
