package org.example;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapReduceTest {
    private static final String TEST_DIR = "test_files";
    private Worker worker;
    private Coordinator coordinator;

    @BeforeAll
    static void setUp() throws IOException {
        Files.createDirectories(Paths.get(TEST_DIR));

        String[] fileContents = {
                "hello world hello java",
                "world java programming",
                "hello programming world",
                "java world test"
        };

        for (int i = 0; i < fileContents.length; i++) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(TEST_DIR + "/file" + i + ".txt"))) {
                writer.println(fileContents[i]);
            }
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (Files.exists(Paths.get(TEST_DIR))) {
            Files.walk(Paths.get(TEST_DIR))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        cleanDirectory("medium");
        cleanDirectory("output");
        cleanDirectory("input");
    }

    private static void cleanDirectory(String dirName) throws IOException {
        Path path = Paths.get(dirName);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @BeforeEach
    void setUpWorker() {
        List<String> dummyFiles = Arrays.asList(TEST_DIR + "/file0.txt");
        coordinator = new Coordinator(dummyFiles, 2);
        worker = new Worker(0, coordinator);
    }

    @Test
    void testCalculateTotalSum() {
        List<KeyValue> keyValues = Arrays.asList(
                new KeyValue("hello", "1"),
                new KeyValue("world", "1"),
                new KeyValue("hello", "1"),
                new KeyValue("java", "1")
        );

        int totalSum = worker.calculateTotalSum(keyValues);
        assertEquals(4, totalSum);
    }

    @Test
    void testCommonResultFileWriting() throws IOException {
        Files.deleteIfExists(Paths.get("output/result-sum.txt"));

        List<String> dummyFiles = Arrays.asList(TEST_DIR + "/file0.txt");
        Coordinator testCoordinator = new Coordinator(dummyFiles, 2);
        Worker testWorker = new Worker(1, testCoordinator);

        testWorker.writeToCommonResultFile(100, 0);

        String content1 = new String(Files.readAllBytes(Paths.get("output/result-sum.txt"))).trim();
        assertEquals("key result: 100", content1);

        testWorker.writeToCommonResultFile(50, 1);

        String content2 = new String(Files.readAllBytes(Paths.get("output/result-sum.txt"))).trim();
        assertEquals("key result: 150", content2);
    }

    @Test
    void testMapFunction() {
        List<String> dummyFiles = Arrays.asList(TEST_DIR + "/file0.txt");
        Coordinator testCoordinator = new Coordinator(dummyFiles, 2);
        Worker testWorker = new Worker(2, testCoordinator);

        String content = "hello world hello java";
        List<KeyValue> result = testWorker.map("test.txt", content);

        assertEquals(4, result.size());

        for (KeyValue kv : result) {
            assertEquals("1", kv.getValue());
            assertTrue(kv.getKey().equals("hello") || kv.getKey().equals("world") || kv.getKey().equals("java"));
        }
    }

    @Test
    void testFullMapReduceWithCommonResult() throws InterruptedException, IOException {
        List<String> inputFiles = Arrays.asList(
                TEST_DIR + "/file0.txt",
                TEST_DIR + "/file1.txt",
                TEST_DIR + "/file2.txt",
                TEST_DIR + "/file3.txt"
        );

        MapReduceFramework framework = new MapReduceFramework(inputFiles, 2, 2);
        framework.execute();

        Path resultFile = Paths.get("output/result-sum.txt");
        assertTrue(Files.exists(resultFile), "Common result file should exist");

        String content = new String(Files.readAllBytes(resultFile)).trim();

        assertEquals("key result: " + 13, content, "Total words count should be 13");
    }
}