package org.example;

import java.util.List;
import java.util.Objects;

public class Task {
    private final TaskType type;
    private final int taskId;
    private final String fileName;
    private final List<String> intermediateFiles;
    private final int numReduceTasks;

    // Private constructor
    private Task(TaskType type, int taskId, String fileName,
                 List<String> intermediateFiles, int numReduceTasks) {
        this.type = type;
        this.taskId = taskId;
        this.fileName = fileName;
        this.intermediateFiles = intermediateFiles;
        this.numReduceTasks = numReduceTasks;
    }

    // Factory methods
    public static Task createMapTask(int taskId, String fileName, int numReduceTasks) {
        return new Task(TaskType.MAP, taskId, fileName, null, numReduceTasks);
    }

    public static Task createReduceTask(int taskId, List<String> intermediateFiles) {
        return new Task(TaskType.REDUCE, taskId, null, intermediateFiles, 0);
    }

    public static Task createWaitTask() {
        return new Task(TaskType.WAIT, -1, null, null, 0);
    }

    public static Task createExitTask() {
        return new Task(TaskType.EXIT, -1, null, null, 0);
    }

    // Getters
    public TaskType getType() { return type; }
    public int getTaskId() { return taskId; }
    public String getFileName() { return fileName; }
    public List<String> getIntermediateFiles() { return intermediateFiles; }
    public int getNumReduceTasks() { return numReduceTasks; }

    // Helper methods
    public boolean isMapTask() { return type == TaskType.MAP; }
    public boolean isReduceTask() { return type == TaskType.REDUCE; }
    public boolean isWaitTask() { return type == TaskType.WAIT; }
    public boolean isExitTask() { return type == TaskType.EXIT; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return taskId == task.taskId &&
               numReduceTasks == task.numReduceTasks &&
               type == task.type &&
               Objects.equals(fileName, task.fileName) &&
               Objects.equals(intermediateFiles, task.intermediateFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, taskId, fileName, intermediateFiles, numReduceTasks);
    }

    @Override
    public String toString() {
        return "Task{" +
               "type=" + type +
               ", taskId=" + taskId +
               ", fileName='" + fileName + '\'' +
               ", intermediateFiles=" + intermediateFiles +
               ", numReduceTasks=" + numReduceTasks +
               '}';
    }
}