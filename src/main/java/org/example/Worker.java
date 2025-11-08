package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Класс Worker предназначен для выполнения MapReduce задач.
 * Worker получает задачи от координатора и выполняет map или reduce операции.
 * Поддерживает многопоточное выполнение задач с синхронизацией доступа к общим ресурсам.
 */
public class Worker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private final Coordinator coordinator;
    private final int workerId;
    private static final ReentrantLock resultFileLock = new ReentrantLock();

    /**
     * Создает новый экземпляр воркера.
     *
     * @param workerId    Уникальный идентификатор воркера
     * @param coordinator Координатор для получения задач и отчетов о статусе
     * @throws IllegalArgumentException если coordinator равен null
     */
    public Worker(int workerId, Coordinator coordinator) {
        if (coordinator == null) {
            throw new IllegalArgumentException("coordinator не может быть null");
        }

        this.workerId = workerId;
        this.coordinator = coordinator;

        logger.info("Worker {} создан", workerId);
    }

    /**
     * Основной метод выполнения воркера.
     * Воркер постоянно запрашивает задачи у координатора и выполняет их до получения EXIT задачи.
     */
    @Override
    public void run() {
        logger.info("Worker {} запущен", workerId);

        while (true) {
            Task task = coordinator.getTask();

            switch (task.getType()) {
                case MAP:
                    processMapTask(task);
                    break;
                case REDUCE:
                    processReduceTask(task);
                    break;
                case WAIT:
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.info("Worker {} прерван", workerId);
                        return;
                    }
                    break;
                case EXIT:
                    logger.info("Worker {} получил сигнал EXIT", workerId);
                    return;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Обрабатывает map задачу: читает файл, применяет map функцию и записывает промежуточные файлы.
     *
     * @param task Map задача для обработки
     */
    private void processMapTask(Task task) {
        logger.info("Worker {} обрабатывает MAP задачу {} для файла: {}", workerId, task.getTaskId(), task.getFileName());

        try {
            File file = new File(task.getFileName());
            if (!file.exists()) {
                logger.error("Файл не найден: {}", task.getFileName());
                return;
            }

            String content = new String(Files.readAllBytes(Paths.get(task.getFileName())));

            List<KeyValue> keyValues = map(task.getFileName(), content);

            List<String> outputFiles = writeIntermediateFiles(keyValues, task.getTaskId(), task.getNumReduceTasks());

            coordinator.completeMapTask(task.getTaskId(), outputFiles);

            logger.info("Worker {} завершил MAP задачу {}, создано {} промежуточных файлов",
                    workerId, task.getTaskId(), outputFiles.size());

        } catch (IOException e) {
            logger.error("Ошибка обработки MAP задачи {}: {}", task.getTaskId(), e.getMessage(), e);
        }
    }

    /**
     * Обрабатывает reduce задачу: читает промежуточные файлы, вычисляет общую сумму и записывает результат.
     *
     * @param task Reduce задача для обработки
     */
    private void processReduceTask(Task task) {
        logger.info("Worker {} обрабатывает REDUCE задачу {} с {} файлами",
                workerId, task.getTaskId(), task.getIntermediateFiles().size());

        try {
            List<KeyValue> allKeyValues = new ArrayList<>();
            for (String file : task.getIntermediateFiles()) {
                if (new File(file).exists()) {
                    List<KeyValue> fileKeyValues = readIntermediateFile(file);
                    allKeyValues.addAll(fileKeyValues);
                    logger.debug("Worker {} прочитал {} пар ключ-значение из {}", workerId, fileKeyValues.size(), file);
                } else {
                    logger.error("Промежуточный файл не найден: {}", file);
                }
            }

            if (allKeyValues.isEmpty()) {
                logger.info("Worker {} не найдено пар ключ-значение для reduce задачи {}", workerId, task.getTaskId());
                coordinator.completeReduceTask(task.getTaskId());
                return;
            }

            int totalSum = calculateTotalSum(allKeyValues);

            writeToCommonResultFile(totalSum, task.getTaskId());

            coordinator.completeReduceTask(task.getTaskId());

            logger.info("Worker {} завершил REDUCE задачу {}, общая сумма: {}", workerId, task.getTaskId(), totalSum);

        } catch (IOException e) {
            logger.error("Ошибка обработки REDUCE задачи {}: {}", task.getTaskId(), e.getMessage(), e);
        }
    }

    /**
     * Вычисляет общую сумму всех значений из списка пар ключ-значение.
     *
     * @param keyValues Список пар ключ-значение для обработки
     * @return Общая сумма всех значений
     */
    public int calculateTotalSum(List<KeyValue> keyValues) {
        int totalSum = 0;
        for (KeyValue kv : keyValues) {
            try {
                totalSum += Integer.parseInt(kv.getValue());
            } catch (NumberFormatException e) {
                logger.warn("Неверный формат значения: {}", kv.getValue());
            }
        }
        return totalSum;
    }

    /**
     * Записывает общую сумму в общий файл результатов с синхронизацией доступа.
     * Использует блокировку для обеспечения потокобезопасности при многопоточном доступе.
     *
     * @param totalSum     Сумма для добавления к общему результату
     * @param reduceTaskId ID reduce задачи для логирования
     * @throws IOException если происходит ошибка ввода-вывода при работе с файлом
     */
    void writeToCommonResultFile(int totalSum, int reduceTaskId) throws IOException {
        resultFileLock.lock();
        try {
            Files.createDirectories(Paths.get("output"));
            String filename = "output/result-sum.txt";

            int currentTotal = 0;
            File resultFile = new File(filename);

            if (resultFile.exists()) {
                try {
                    String content = new String(Files.readAllBytes(Paths.get(filename))).trim();
                    if (content.startsWith("key result: ")) {
                        String numberStr = content.substring("key result: ".length()).trim();
                        currentTotal = Integer.parseInt(numberStr);
                    } else {
                        currentTotal = Integer.parseInt(content);
                    }
                } catch (Exception e) {
                    logger.warn("Ошибка чтения файла результатов, начинаем заново: {}", e.getMessage());
                    currentTotal = 0;
                }
            }

            int newTotal = currentTotal + totalSum;

            try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                writer.print("key result: " + newTotal);
            }

            logger.info("Worker {} добавил {} к результату (было: {}, стало: {}) для reduce задачи {}",
                    workerId, totalSum, currentTotal, newTotal, reduceTaskId);

        } finally {
            resultFileLock.unlock();
        }
    }

    /**
     * Map функция: разбивает содержимое на слова и возвращает пары ключ-значение.
     * Каждое слово преобразуется в нижний регистр и получает значение "1".
     *
     * @param fileName Имя обрабатываемого файла
     * @param content  Содержимое файла для обработки
     * @return Список пар ключ-значение (слово -> "1")
     */
    public List<KeyValue> map(String fileName, String content) {
        List<KeyValue> result = new ArrayList<>();

        // Разбиваем содержимое на слова
        String[] words = content.split("\\W+");

        for (String word : words) {
            if (!word.trim().isEmpty()) {
                result.add(new KeyValue(word.toLowerCase(), "1"));
            }
        }

        logger.debug("Map функция обработала {} слов из файла {}", result.size(), fileName);
        return result;
    }

    /**
     * Записывает промежуточные файлы, группируя пары ключ-значение по reduce задачам.
     *
     * @param keyValues      Список пар ключ-значение для записи
     * @param mapTaskId      ID map задачи
     * @param numReduceTasks Количество reduce задач
     * @return Список созданных промежуточных файлов
     * @throws IOException если происходит ошибка ввода-вывода при записи файлов
     */
    private List<String> writeIntermediateFiles(List<KeyValue> keyValues, int mapTaskId, int numReduceTasks) throws IOException {
        Files.createDirectories(Paths.get("medium"));

        Map<Integer, List<KeyValue>> grouped = new HashMap<>();
        for (int i = 0; i < numReduceTasks; i++) {
            grouped.put(i, new ArrayList<>());
        }

        for (KeyValue kv : keyValues) {
            int reduceId = Math.abs(kv.getKey().hashCode()) % numReduceTasks;
            grouped.get(reduceId).add(kv);
        }

        List<String> outputFiles = new ArrayList<>();
        for (Map.Entry<Integer, List<KeyValue>> entry : grouped.entrySet()) {
            String filename = "medium/mr-" + mapTaskId + "-" + entry.getKey() + ".txt";
            try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                for (KeyValue kv : entry.getValue()) {
                    writer.println(kv.getKey() + " " + kv.getValue());
                }
            }
            outputFiles.add(filename);
        }

        logger.debug("Создано {} промежуточных файлов для map задачи {}", outputFiles.size(), mapTaskId);
        return outputFiles;
    }

    /**
     * Читает пары ключ-значение из промежуточного файла.
     *
     * @param filename Имя файла для чтения
     * @return Список пар ключ-значение из файла
     * @throws IOException если происходит ошибка ввода-вывода при чтении файла
     */
    private List<KeyValue> readIntermediateFile(String filename) throws IOException {
        List<KeyValue> result = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                if (parts.length == 2) {
                    result.add(new KeyValue(parts[0], parts[1]));
                }
            }
        }
        return result;
    }

    /**
     * Группирует пары ключ-значение по ключу.
     *
     * @param keyValues Список пар ключ-значение для группировки
     * @return Map с ключами и списками соответствующих значений
     */
    private Map<String, List<String>> groupByKey(List<KeyValue> keyValues) {
        Map<String, List<String>> result = new HashMap<>();
        for (KeyValue kv : keyValues) {
            result.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).add(kv.getValue());
        }
        return result;
    }
}