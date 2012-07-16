import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;
import org.xml.sax.helpers.DefaultHandler;

public class WorkerThread implements Runnable {

	private final Text logTextCtrl;

	public WorkerThread(Text logTextCtrl) {
		this.logTextCtrl = logTextCtrl;
	}

	@Override
	public void run() {
		final String rootDirName = "mediation_streams";
		RecordFieldList fileSchema = new RecordFieldList();

		DefaultHandler handler = new SAXParserHandler(fileSchema);
		// Use the default (non-validating) parser
		SAXParserFactory factory = SAXParserFactory.newInstance();
		try {
			// Parse the input
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(new File(rootDirName + "/nsn_r4_schema.xml"), handler);
		} catch (Throwable t) {
			t.printStackTrace();
		}

		Pattern pattern = Pattern.compile(fileSchema.getFileNamePattern());
		Matcher matcher = null;
		String dirName = rootDirName + "/old_system/Output" + fileSchema.getOutputDir();
		// Vector<String> filesToProcess = new Vector<String>();
		List<String> filesToProcess = new ArrayList<String>();
		File dir = new File(dirName);
		File listDir[] = dir.listFiles();
		for (int i = 0; i < listDir.length; i++) {
			matcher = pattern.matcher(listDir[i].getName());
			if (matcher.find())
				filesToProcess.add(listDir[i].getName());
		}

		Collections.sort(filesToProcess);

		boolean tasksEnded = false;

		ExecutorService threadExecutor;

		long sw = System.currentTimeMillis();

		final int ITERATIONS = 1;
		try {
			for (int i = 0; i < ITERATIONS; i++) {
				if (ITERATIONS < 10000 || i % 1000 == 0) {
					logMessage(String.format("Iteration N: %d -- %s\n", i + 1, new Date()));
					logMessage(String.format("==================================================\n\n"));
				}

				threadExecutor = Executors.newCachedThreadPool();
				Iterator<String> it = filesToProcess.iterator();
				while (it.hasNext()) {
					// ReadFile task1 = new ReadFile(textCtrl, it.next());
					// threadExecutor.execute(new ReadFile(textCtrl, it.next()));
					// threadExecutor.execute(new ReadFile(textCtrl, it.next()));
					// threadExecutor.execute(new ReadFile(textCtrl, it.next()));
					// it.next();
					threadExecutor.execute(new ReadFile(logTextCtrl, rootDirName, it.next(), fileSchema));
					// threadExecutor.execute(new ReadFile(textCtrl, it.next(), fileSchema));
					// threadExecutor.execute(new ReadFile(textCtrl, it.next(), fileSchema));
				}

				// shut down worker threads when their tasks complete
				threadExecutor.shutdown();

				// Thread t1 = new Thread(new ReadFile(textCtrl, it.next(), fileSchema));
				// Thread t2 = new Thread(new ReadFile(textCtrl, it.next(), fileSchema));
				// t1.start();
				// t2.start();

				try { // wait 1 minute for both writers to finish executing
					tasksEnded = threadExecutor.awaitTermination(1, TimeUnit.MINUTES);
				} catch (InterruptedException ex) {
					logMessage("Interrupted while wait for tasks to finish.\n");
				}
			}

			if (tasksEnded)
				logMessage(String.format("\n <<<<<<<<<<<<<<<< End : %d ms  -- %s >>>>>>>>>>>>>>>>>>>\n", System
						.currentTimeMillis()
						- sw, new Date()));
			else
				logMessage("Timed out while waiting for tasks to finish.\n");
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

		// if (Display.getCurrent() != null) {
		// r.run();
		// } else {
		Display.getDefault().asyncExec(r);
		// Display.getDefault().syncExec(r);
		// }
	}

}
