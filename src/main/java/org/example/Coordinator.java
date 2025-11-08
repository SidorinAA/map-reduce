package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Класс Coordinator отвечает за управление распределением задач MapReduce и отслеживание прогресса.
 * Данный класс координирует выполнение map и reduce задач между несколькими воркерами,
 * поддерживает очереди задач, отслеживает статус завершения и управляет инициализацией задач.
 */
public class Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private final Queue<Task> mapTasks;
    private final Queue<Task> reduceTasks;
    private final Set<Integer> completedMapTasks;
    private final Set<Integer> completedReduceTasks;
    private final Map<Integer, List<String>> mapTaskOutputs;
    private final int numReduceTasks;
    private final AtomicInteger mapTasksCompleted;
    private final AtomicInteger reduceTasksCompleted;
    private volatile boolean allMapTasksDone;
    private volatile boolean allReduceTasksDone;
    private final int totalMapTasks;
    private boolean reduceTasksInitialized = false;

    /**
     * Создает новый Coordinator с указанными входными файлами и количеством reduce задач.
     *
     * @param inputFiles     Список путей к входным файлам для обработки map задачами
     * @param numReduceTasks Количество reduce задач для создания в рамках job
     * @throws IllegalArgumentException если inputFiles равен null или numReduceTasks меньше 1
     */
    public Coordinator(List<String> inputFiles, int numReduceTasks) {
        if (inputFiles == null) {
            throw new IllegalArgumentException("inputFiles не может быть null");
        }
        if (numReduceTasks < 1) {
            throw new IllegalArgumentException("numReduceTasks должен быть не менее 1");
        }

        this.mapTasks = new ConcurrentLinkedQueue<>();
        this.reduceTasks = new ConcurrentLinkedQueue<>();
        this.completedMapTasks = ConcurrentHashMap.newKeySet();
        this.completedReduceTasks = ConcurrentHashMap.newKeySet();
        this.mapTaskOutputs = new ConcurrentHashMap<>();
        this.numReduceTasks = numReduceTasks;
        this.mapTasksCompleted = new AtomicInteger(0);
        this.reduceTasksCompleted = new AtomicInteger(0);
        this.allMapTasksDone = false;
        this.allReduceTasksDone = false;

        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.offer(Task.createMapTask(i, inputFiles.get(i), numReduceTasks));
        }
        this.totalMapTasks = inputFiles.size();

        logger.info("Coordinator инициализирован с {} map задачами и {} reduce задачами", totalMapTasks, numReduceTasks);
    }

    /**
     * Получает следующую доступную задачу для воркера.
     *
     * Порядок приоритета:
     * 1. Map задачи (если доступны)
     * 2. Reduce задачи (если все map задачи завершены)
     * 3. Задача ожидания (если задачи выполняются, но в данный момент нет доступных)
     * 4. Задача выхода (если все задачи завершены)
     *
     * @return Следующая Task для выполнения, которая может быть:
     *         - Map задачей
     *         - Reduce задачей
     *         - Задачей ожидания (если нет доступных задач, но job в процессе выполнения)
     *         - Задачей выхода (если все задачи завершены)
     */
    public synchronized Task getTask() {
        if (!mapTasks.isEmpty()) {
            return mapTasks.poll();
        }

        if (!allMapTasksDone) {
            return Task.createWaitTask();
        }

        if (!reduceTasks.isEmpty()) {
            return reduceTasks.poll();
        }

        if (!allReduceTasksDone) {
            return Task.createWaitTask();
        }

        return Task.createExitTask();
    }

    /**
     * Отмечает map задачу как завершенную и записывает ее выходные файлы.
     * Когда все map задачи завершены, автоматически инициализирует reduce задачи.
     *
     * @param taskId      ID завершенной map задачи
     * @param outputFiles Список выходных файлов, созданных map задачей
     * @throws IllegalArgumentException если taskId невалиден или outputFiles равен null
     */
    public synchronized void completeMapTask(int taskId, List<String> outputFiles) {
        if (taskId < 0 || taskId >= totalMapTasks) {
            throw new IllegalArgumentException("Невалидный ID map задачи: " + taskId);
        }
        if (outputFiles == null) {
            throw new IllegalArgumentException("outputFiles не может быть null");
        }

        completedMapTasks.add(taskId);
        mapTaskOutputs.put(taskId, outputFiles);
        int completed = mapTasksCompleted.incrementAndGet();

        logger.info("Map задача {} завершена. Прогресс: {}/{}", taskId, completed, totalMapTasks);

        if (completed == totalMapTasks && mapTasks.isEmpty()) {
            allMapTasksDone = true;
            logger.info("Все map задачи завершены. Инициализация reduce задач...");
            initializeReduceTasks();
        }
    }

    /**
     * Отмечает reduce задачу как завершенную.
     * Когда все reduce задачи завершены, отмечает всю работу как завершенную.
     *
     * @param taskId ID завершенной reduce задачи
     * @throws IllegalArgumentException если taskId невалиден
     */
    public synchronized void completeReduceTask(int taskId) {
        if (taskId < 0 || taskId >= numReduceTasks) {
            throw new IllegalArgumentException("Невалидный ID reduce задачи: " + taskId);
        }

        completedReduceTasks.add(taskId);
        int completed = reduceTasksCompleted.incrementAndGet();

        logger.info("Reduce задача {} завершена. Прогресс: {}/{}", taskId, completed, numReduceTasks);

        if (completed == numReduceTasks && reduceTasks.isEmpty()) {
            allReduceTasksDone = true;
            logger.info("Все reduce задачи завершены. Job завершен!");
        }
    }

    /**
     * Инициализирует reduce задачи путем группировки выходных файлов map задач по их reduce партициям.
     * Этот метод вызывается автоматически, когда все map задачи завершены.
     * Каждая reduce задача получает промежуточные файлы, которые принадлежат ее партиции.
     */
    private synchronized void initializeReduceTasks() {
        if (reduceTasksInitialized) {
            return;
        }
        reduceTasksInitialized = true;

        Map<Integer, List<String>> reduceFiles = new HashMap<>();
        for (int i = 0; i < numReduceTasks; i++) {
            reduceFiles.put(i, new ArrayList<>());
        }

        for (List<String> files : mapTaskOutputs.values()) {
            for (String file : files) {
                String[] parts = file.split("-");
                if (parts.length >= 3) {
                    try {
                        int reduceId = Integer.parseInt(parts[2].split("\\.")[0]);
                        reduceFiles.get(reduceId).add(file);
                    } catch (NumberFormatException e) {
                        logger.error("Неверный формат имени файла: {}", file);
                    }
                }
            }
        }

        for (int i = 0; i < numReduceTasks; i++) {
            List<String> files = reduceFiles.get(i);
            logger.info("Reduce задача {} будет обрабатывать {} файлов", i, files.size());
            reduceTasks.offer(Task.createReduceTask(i, files));
        }
    }

    /**
     * Возвращает статус завершения всех map задач.
     *
     * @return true если все map задачи завершены, false в противном случае
     */
    public boolean isAllMapTasksDone() {
        return allMapTasksDone;
    }

    /**
     * Возвращает статус завершения всех reduce задач.
     *
     * @return true если все reduce задачи завершены, false в противном случае
     */
    public boolean isAllReduceTasksDone() {
        return allReduceTasksDone;
    }

    /**
     * Возвращает количество reduce задач, настроенных для этого job.
     *
     * @return количество reduce задач
     */
    public int getNumReduceTasks() {
        return numReduceTasks;
    }
}