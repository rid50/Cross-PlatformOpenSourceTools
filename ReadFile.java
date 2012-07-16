import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;

public class ReadFile implements Runnable {

	private final int NUM_RECORDS_TO_READ = 1000;
	private final Text logTextCtrl;
	private final String rootDirName;
	private final String fileName;
	private final RecordFieldList fileSchema;
	private final StringBuilder log = new StringBuilder();

	public ReadFile(Text logTextCtrl, String rootDirName, String fileName, RecordFieldList fileSchema) {
		// public ReadFile(Text logTextCtrl, String fileName) {
		this.logTextCtrl = logTextCtrl;
		this.rootDirName = new String(rootDirName);
		this.fileName = new String(fileName);
		// this.fileSchema = fileSchema;
		this.fileSchema = (RecordFieldList) fileSchema.clone();

//		this.fileSchema = new RecordFieldList();
//		this.fileSchema.setFileNamePattern(fileSchema.getFileNamePattern());
//		this.fileSchema.setOutputDir(fileSchema.getOutputDir());
//		Vector<FieldNameLength> v = new Vector<FieldNameLength>();
//		Iterator<FieldNameLength> it = fileSchema.getVector().iterator();
//		while (it.hasNext()) {
//			FieldNameLength fn = new FieldNameLength();
//			FieldNameLength fnIt = it.next();
//			fn.setName(fnIt.getName());
//			fn.setLen(fnIt.getLen());
//			fn.setKey(fnIt.getKey());
//			fn.setBlack(fnIt.getBlack());
//			v.add(fn);
//		}
//		this.fileSchema.setVector(v);
	}

	@Override
	public void run() {
		try {
			// logMessage(String.format("File name: %s\n", fileName));
			compare(rootDirName, fileName, fileSchema);
			// Thread.sleep(100);
		} catch (Exception exception) {
			logMessage("Task terminated prematurely due to interruption\n");
		}
	}

	void logMessage(final String message) {
		Runnable r = new Runnable() {
			public void run() {
				logTextCtrl.append(message);
			}
		};

		if (Display.getCurrent() != null) {
			r.run();
		} else {
			Display.getDefault().asyncExec(r);
			// Display.getDefault().syncExec(r);
		}
	}

	void compare(String rootDirName, String fileName, RecordFieldList fileSchema) {

//		XMLDecoder decoder = null;
//		RecordFieldList schema = null;
//		try {
//			FileInputStream os = new FileInputStream("C:/mediation_streams/nsn_r4_schema.xml");
//			decoder = new XMLDecoder(os);
//			schema = (RecordFieldList) decoder.readObject();
//		} catch (FileNotFoundException ex) {
//			ex.printStackTrace();
//		} catch (IOException ex) {
//			ex.printStackTrace();
//		} finally {
//			if (decoder != null)
//				decoder.close();
//		}

//		RecordFieldList fileSchema = new RecordFieldList();
//
//		DefaultHandler handler = new SAXParserHandler(fileSchema);
//		// Use the default (non-validating) parser
//		SAXParserFactory factory = SAXParserFactory.newInstance();
//		try {
//			// Parse the input
//			SAXParser saxParser = factory.newSAXParser();
//			saxParser.parse(new File("C:/mediation_streams/nsn_r4_schema.xml"), handler);
//		} catch (Throwable t) {
//			t.printStackTrace();
//		}

		Vector<FieldNameLength> schema = fileSchema.getVector();

		int key_size = 0, value_size = 0;
		int schema_size = schema.size();
		int record_size = 1; // line feed (0x0A) at the end of the file
		// get an Iterator object for Vector using iterator() method.
		Iterator<FieldNameLength> it = schema.iterator();
		while (it.hasNext()) {
			FieldNameLength fn = it.next();
			if (fn.getKey()) {
				key_size += fn.getLen();
			} else if (!fn.getBlack()) {
				value_size += fn.getLen();
			}

			// record_size += it.next().getLen();
			record_size += fn.getLen();
		}

		// String fileName = "NSN01_1201230734_0192T";
		// String dirName = "C:/mediation_streams/old_system/Output/MSC_R99/To_Billing";
		String dirName = rootDirName + "/old_system/Output" + fileSchema.getOutputDir();
		String dirName2 = rootDirName + "/new_system/Output" + fileSchema.getOutputDir();

		// logMessage(String.format("File name: %s\n", dirName + fileName));
		log.append(String.format("File name: %s\n", dirName + fileName));

		File file = new File(dirName, fileName);
		File file2 = new File(dirName2, fileName);

		BufferedInputStream bufferedInput = null;
		BufferedInputStream bufferedInput2 = null;
		// int recordSize = 641;
		byte[] buffer = new byte[record_size * NUM_RECORDS_TO_READ];
		byte[] buffer2 = new byte[record_size * NUM_RECORDS_TO_READ];

		try {
			bufferedInput = new BufferedInputStream(new FileInputStream(file));
			bufferedInput2 = new BufferedInputStream(new FileInputStream(file2));

			int bytesRead = 0, bytesRead2 = 0;
			int n = 0;
			int offsetFile = 0, offsetRec = 0, offsetKey = 0, offsetValue = 0;
			int dupKeys = 0;
			long sw = System.currentTimeMillis();

			// HashMap<String, String> hashTable = new HashMap<String, String>();
			// HashMap<String, String> hashTable2 = new HashMap<String, String>();
			HashMap<ByteArrayWrapper, byte[]> hashTable = new HashMap<ByteArrayWrapper, byte[]>();
			HashMap<ByteArrayWrapper, byte[]> hashTable2 = new HashMap<ByteArrayWrapper, byte[]>();
//			StringBuilder keyFields = new StringBuilder();
//			StringBuilder keyFields2 = new StringBuilder();
//			StringBuilder valueFields = new StringBuilder();
//			StringBuilder valueFields2 = new StringBuilder();
			byte[] keyFields = new byte[key_size];
			byte[] keyFields2 = new byte[key_size];
			byte[] valueFields = new byte[value_size];
			byte[] valueFields2 = new byte[value_size];

			// Keep reading from the file while there is any content
			// when the end of the stream has been reached, -1 is returned
			while ((bytesRead = bufferedInput.read(buffer)) != -1 && (bytesRead2 = bufferedInput2.read(buffer2)) != -1) {
				int numRec = bytesRead / record_size;
				offsetFile = 0;
				for (int i = 0; i < numRec; i++) {
					++n;

					offsetRec = 0;
					offsetKey = 0;
					offsetValue = 0;
//					keyFields.delete(0, keyFields.length());
//					keyFields2.delete(0, keyFields2.length());
//					valueFields.delete(0, valueFields.length());
//					valueFields2.delete(0, valueFields2.length());
					it = schema.iterator();
					while (it.hasNext()) {
						FieldNameLength fn = (FieldNameLength) it.next();
						if (fn.getKey()) {
							// keyFields.append(new String(buffer, offsetFile + offsetRec, fn.getLen()));
							// keyFields2.append(new String(buffer2, offsetFile + offsetRec, fn.getLen()));
							System.arraycopy(buffer, offsetFile + offsetRec, keyFields, offsetKey, fn.getLen());
							System.arraycopy(buffer2, offsetFile + offsetRec, keyFields2, offsetKey, fn.getLen());
							offsetKey += fn.getLen();
						} else if (!fn.getBlack()) {
							// valueFields.append(new String(buffer, offsetFile + offsetRec, fn.getLen()));
							// valueFields2.append(new String(buffer2, offsetFile + offsetRec, fn.getLen()));
							System.arraycopy(buffer, offsetFile + offsetRec, valueFields, offsetValue, fn.getLen());
							System.arraycopy(buffer2, offsetFile + offsetRec, valueFields2, offsetValue, fn.getLen());
							offsetValue += fn.getLen();
						}
						offsetRec += fn.getLen();
					}

					offsetFile += record_size;

					// if (true)
					// continue;

					// if (hashTable.containsKey(keyFields.toString())) {
					// if (hashTable.containsKey(new String(keyFields))) {
					ByteArrayWrapper wrapper = new ByteArrayWrapper(keyFields);
					if (hashTable.containsKey(wrapper)) {
						// logMessage(String.format("Duplicate key in file: %s\n", dirName));
						// logMessage(String.format("Key field: %s\n", keyFields.toString()));
						log.append(String.format("Duplicate key in file: %s\n", dirName + fileName));
						log.append(String.format("Key field: %s\n", new String(keyFields)));
						dupKeys++;
						continue;
					} else
						hashTable.put(wrapper, valueFields.clone());
					// hashTable.put(new String(keyFields), new String(valueFields));
					// hashTable.put(keyFields.toString(), valueFields.toString());

					ByteArrayWrapper wrapper2 = new ByteArrayWrapper(keyFields2);
					if (hashTable2.containsKey(wrapper2)) {
						// logMessage(String.format("Duplicate key in file: %s\n", dirName2));
						// logMessage(String.format("Key field: %s\n", keyFields2.toString()));
						log.append(String.format("Duplicate key in file: %s\n", dirName2 + fileName));
						log.append(String.format("Key field: %s\n", new String(keyFields2)));
					} else
						hashTable2.put(wrapper2, valueFields2.clone());
					// hashTable2.put(new String(keyFields2), new String(valueFields2));
					// hashTable2.put(keyFields2.toString(), valueFields2.toString());
				}
			}

			// if (true)
			// return;

			// Set<String> set = hashTable.keySet();
			// Iterator<String> itk = set.iterator();
			// String key = "";

			Set<ByteArrayWrapper> set = hashTable.keySet();

			// log.append(String.format("Key field: %s\n", hashTable.));

			Iterator<ByteArrayWrapper> itk = set.iterator();
			// byte[] key = new byte[key_size];
			ByteArrayWrapper key;
			while (itk.hasNext()) {
				key = itk.next();
				if (hashTable2.containsKey(key)) {
					// log.append(String.format("Value field: %s,\n\t\n", new String(hashTable.get(key))));
					if (!(Arrays.equals(hashTable.get(key), hashTable2.get(key)))) {
						offsetRec = 0;
						Iterator<FieldNameLength> its = schema.iterator();
						while (its.hasNext()) {
							FieldNameLength fn = its.next();
							if (!(fn.getKey() || fn.getBlack())) {
								// String str = hashTable.get(key).substring(offsetRec, offsetRec + fn.getLen());
								// String str2 = hashTable2.get(key).substring(offsetRec, offsetRec + fn.getLen());
								// if (!(str.equals(str2))) {
								// if (true)
								// break;
								if (!(Arrays.equals(Arrays.copyOfRange(hashTable.get(key), offsetRec, offsetRec
										+ fn.getLen()), Arrays.copyOfRange(hashTable2.get(key), offsetRec, offsetRec
										+ fn.getLen())))) {
									// logMessage(String.format(
									log.append(String
											.format("Fields %s do not match, key field: %s,\n\t old value: %s, new value: %s\n",
													fn.getName(), new String(key.getBytes()), new String(Arrays
															.copyOfRange(hashTable.get(key), offsetRec, offsetRec
																	+ fn.getLen())), new String(Arrays.copyOfRange(
															hashTable2.get(key), offsetRec, offsetRec + fn.getLen()))));
									// fn.getName(), key, str, str2));
								}
								offsetRec += fn.getLen();
							}
						}
					}
				} else {
					// logMessage(String.format("No record found: key field: %s\n", key));
					log.append(String.format("No record found: key field: %s\n", new String(key.getBytes())));
				}
			}

			// if (bytesRead == recordSize * NUM_RECORDS_TO_READ)
			// ++i;
			// else
			// logMessage(String.format("Last record size: %d\n", bytesRead));
			// Process the chunk of bytes read
			// in this case we just construct a String and print it out
			// String chunk = new String(buffer, 0, bytesRead);
			// logMessage(chunk);
			// System.out.print(chunk);

			// logMessage(String.format("Time to process the file: %d ms\n", System.currentTimeMillis() - sw));
			// logMessage(String.format("Number of duplicate keys: %d\n", dupKeys));
			// logMessage(String.format("Number of records read: %d\n", n));
			// logMessage(String.format("File size calculated (%d x %d): %d\n", record_size, n, record_size * n));

			log.append(String.format("Time to process the file: %d ms\n", System.currentTimeMillis() - sw));
			log.append(String.format("Number of duplicate keys: %d\n", dupKeys));
			log.append(String.format("Number of records read: %d\n", n));
			log.append(String.format("File size calculated (%d x %d): %d\n", record_size, n, record_size * n));
			log.append("---------------------------------------------------------------------------------------------\n");

			logMessage(log.toString());

		} catch (FileNotFoundException ex) {
			logMessage(String.format(ex.getMessage() + "\n"));
		} catch (IOException ex) {
			logMessage(String.format(ex.getMessage() + "\n"));
		} finally {
			// Close the BufferedInputStream
			try {
				if (bufferedInput != null)
					bufferedInput.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
}
