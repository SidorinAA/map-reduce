package org.example;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class MapReduceFramework {
    private final List<String> inputFiles;
    private final int numWorkers;
    private final int numReduceTasks;

    public MapReduceFramework(List<String> inputFiles, int numWorkers, int numReduceTasks) {
        this.inputFiles = inputFiles;
        this.numWorkers = numWorkers;
        this.numReduceTasks = numReduceTasks;
    }

    public void execute() throws InterruptedException {
        // Clean up previous results
        cleanupPreviousResults();

        // Create coordinator
        Coordinator coordinator = new Coordinator(inputFiles, numReduceTasks);

        // Create worker threads
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < numWorkers; i++) {
            futures.add(executor.submit(new Worker(i, coordinator)));
        }

        // Wait for all workers to complete
        while (!coordinator.isAllReduceTasksDone()) {
            Thread.sleep(100);
        }

        // Send exit signal to all workers by shutting down executor
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        // Read and display final result
        displayFinalResult();

        System.out.println("MapReduce job completed successfully!");
    }

    private void cleanupPreviousResults() {
        try {
            // Clean up output directory
            java.nio.file.Path outputDir = java.nio.file.Paths.get("output");
            if (java.nio.file.Files.exists(outputDir)) {
                java.nio.file.Files.walk(outputDir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .map(java.nio.file.Path::toFile)
                        .forEach(File::delete);
            }

            // Clean up medium directory
            java.nio.file.Path mediumDir = java.nio.file.Paths.get("medium");
            if (java.nio.file.Files.exists(mediumDir)) {
                java.nio.file.Files.walk(mediumDir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .map(java.nio.file.Path::toFile)
                        .forEach(File::delete);
            }
        } catch (Exception e) {
            System.err.println("Error cleaning up previous results: " + e.getMessage());
        }
    }

    private void displayFinalResult() {
        try {
            java.nio.file.Path resultFile = java.nio.file.Paths.get("output/result-sum.txt");
            if (java.nio.file.Files.exists(resultFile)) {
                String content = new String(java.nio.file.Files.readAllBytes(resultFile)).trim();
                System.out.println("====================================");
                System.out.println("FINAL RESULT: " + content);
                System.out.println("Total words count: " + content);
                System.out.println("====================================");
            } else {
                System.out.println("Result file not found: output/result-sum.txt");
            }
        } catch (Exception e) {
            System.err.println("Error reading final result: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java MapReduceFramework <inputFiles...> <numWorkers> <numReduceTasks>");
            System.out.println("Example: java MapReduceFramework file1.txt file2.txt 2 2");
            return;
        }

        List<String> inputFiles = new ArrayList<>();
        int numWorkers = Integer.parseInt(args[args.length - 2]);
        int numReduceTasks = Integer.parseInt(args[args.length - 1]);

        for (int i = 0; i < args.length - 2; i++) {
            inputFiles.add(args[i]);
        }

        try {
            MapReduceFramework framework = new MapReduceFramework(inputFiles, numWorkers, numReduceTasks);
            framework.execute();
        } catch (InterruptedException e) {
            System.err.println("MapReduce job interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}