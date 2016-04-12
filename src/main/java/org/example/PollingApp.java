package org.example;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class PollingApp {
    public static void main( String ... args ) {
    	if (args.length == 0) {
    		System.err.println("Aaaaargs, MOAR aaaaaargs");
    		System.exit(-1);
    	}
    	
    	new Poller(args[0], file -> System.out.println("--> Omnomming the file to set an example " + file));
    }
    
    public static class Poller {

    	private boolean running = true;
    	private long last = System.currentTimeMillis();
    	
    	private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
    	private long timeoutValue = 500;
    	
    	public boolean isHealthy() {
    		return System.currentTimeMillis() - last < (timeoutUnit.toMillis(timeoutValue) * 10);
    	}
    	
    	public Poller(String path, Consumer<Path> processor) {
    		System.out.println("Polling '" + path + "' every " + timeoutValue + " " + timeoutUnit);

    		// NOTE: Using a random UUID makes it harder to discover "lost" files in case of node-failures etc.
    		//       Using a node-/host-name might be better in some cases to get a consistent name for each instance
    		String consumerId = UUID.randomUUID().toString().replaceAll("-", "");
    		
    		Path pollPath = Paths.get(path);
    		ConcurrentConsumer omnommer = new ConcurrentConsumer(pollPath, consumerId, processor);
    		while (running) {
    			try (Stream<Path> candidates = Files.list(pollPath)) {
    				candidates
	    				.filter(Files::isRegularFile)
	    				.forEach(omnommer);
    				
    				last = System.currentTimeMillis(); 
    				timeoutUnit.sleep(timeoutValue);
    				
    			} catch (InterruptedException e) {
    				System.err.println("Thread interrupted! " + e.getMessage());
    				Thread.currentThread().interrupt();
    				
    			} catch (Throwable t) {
    				System.err.println("Exception when polling " + t.getMessage() + ", ignoring for now!");
    			}
    		}
    	}
    }

    public static class ConcurrentConsumer implements Consumer<Path> {
    	
    	private String consumerId;
    	private Path working;
    	private Path processed;
    	
    	private Consumer<Path> processor;
    	
    	public ConcurrentConsumer(Path source, String consumerId, Consumer<Path> processor) {
    		this.processor = processor;
    		this.consumerId = consumerId;

    		working = createIfNeeded(source.resolve("working"));
    		processed = createIfNeeded(source.resolve("processed"));
    		
    		System.out.println("Concurrent consumer named " + consumerId + " ready for omnom!");
    	}
    	
    	private Path createIfNeeded(Path path) {
    		if (Files.exists(path)) {
    			return path;
    		}
    			
			try {
				System.out.println("'" + path + "' does not exist, creating it now!");
				return Files.createDirectory(path);
			} catch (FileAlreadyExistsException ex) {
				return path;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
    	}
    	
		@Override
		public void accept(Path file) {
			System.out.println("Found: " + file);
			
			try {
				// Claim the file with the instance-id and move out of the polling-directory
				Path claimed = Files.move(file, working.resolve(claim(file)), ATOMIC_MOVE);
				if (Files.exists(claimed)) {
					System.out.println("Success! File claimed as " + claimed);

					// Process the file - typically this will be transactional.
					// Note: this means that the transaction will most likely not be rolled back
					//       if the move-to-processed operation fails.. errors have to be looked into manually.
					processor.accept(claimed);
					
					// All done, moving the file to processed. Any existing files are replaced
					Path done = Files.move(claimed, processed.resolve(file.getFileName()), REPLACE_EXISTING);
					System.out.println("File processed and archived at '" + done + "'");
				} else {
					System.err.println("Oh no! Someone took my file!");
				}
			} catch (NoSuchFileException ne) {
				System.err.println("Oh no! Someone took my file!");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
		}
		
		private String claim(Path file) {
			return consumerId + "." + file.getFileName();
		}
    }
}
