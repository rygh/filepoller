package org.example;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.file.Files.exists;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PollingAppTest implements Consumer<Path> {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();
	
	private Path source;
	private List<String> workItems;
	
	@Before
	public void setup() throws IOException {
		source = temp.newFolder().toPath();
		workItems = new LinkedList<>();
	}
	
	@Test
	public void foldersShouldBeCreated() {
		new PollingApp.ConcurrentConsumer(source, "node-1", this);
		
		assertTrue(exists(source.resolve("working")));
		assertTrue(exists(source.resolve("processed")));
	}
	
	@Test
	public void shouldProcessAndArchiveFiles() throws IOException {
		Path test1 = Files.createFile(source.resolve("test1.txt"));
		Path test2 = Files.createFile(source.resolve("test2.txt"));
		
		Arrays.asList(test1, test2)
			.forEach(new PollingApp.ConcurrentConsumer(source, "node-1", this));
		
		assertTrue(exists(source.resolve("processed/test1.txt")));
		assertTrue(exists(source.resolve("processed/test2.txt")));
	
		assertThat(workItems, hasItems("node-1.test1.txt", "node-1.test2.txt"));
	}
	
	@Test
	public void shouldPollTheSource() throws Exception {
		Files.createFile(source.resolve("test1.txt"));
		Files.createFile(source.resolve("test2.txt"));

		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				new PollingApp.Poller(source.toString(), PollingAppTest.this);
			}
		});
		t.start();
		
		// Meeeeeeeh... create pluggable thread-blocker to prevent this stupid shit
		TimeUnit.MILLISECONDS.sleep(600);
		t.interrupt();
		
		assertTrue(exists(source.resolve("processed/test1.txt")));
		assertTrue(exists(source.resolve("processed/test2.txt")));
	}
	
	@Test
	public void ignoreLostReadsNoException() {
		Path thisFileIsAlreadyMoved = source.resolve("test1.txt");
		
		new PollingApp.ConcurrentConsumer(source, "node-1", this).accept(thisFileIsAlreadyMoved);
		
		assertFalse(exists(source.resolve("processed/test1.txt")));
	}
	

	@Override
	public void accept(Path t) {
		workItems.add(t.getFileName().toString());
	}
	
}
