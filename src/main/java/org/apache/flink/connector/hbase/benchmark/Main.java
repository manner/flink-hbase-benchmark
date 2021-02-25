package org.apache.flink.connector.hbase.benchmark;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        for (RunConfig runConfig : allRunConfigurations()) {
            new Run(runConfig).run();
        }
    }

    public static class RunConfig {
        public final int numberOfColumns;
        public final int parallelism;
        public final BenchmarkGoal goal;
        public final BenchmarkTarget target;

        public RunConfig(int numberOfColumns, int parallelism, BenchmarkGoal goal, BenchmarkTarget target) {
            this.numberOfColumns = numberOfColumns;
            this.parallelism = parallelism;
            this.goal = goal;
            this.target = target;
        }

        public void run() {
        }
    }

    public static class Run {
        public final RunConfig config;
        private String[] tableNames;

        public Run(RunConfig config) {
            this.config = config;
        }

        public void run() {
            clearReplications();
            clearTables();
            tableNames = config.target.createTables();
            setupFlinkEnvironment();
            waitForTermination();
            retrieveResults();
        }


        private void clearReplications() {
        }

        private void clearTables() {
        }

        private void setupFlinkEnvironment() {
        }

        private void waitForTermination() {
        }

        private void retrieveResults() {

        }
    }

    public static abstract class BenchmarkGoal {
        public static class Througput extends BenchmarkGoal {}

        public static class Latency extends BenchmarkGoal {}
    }

    public static abstract class BenchmarkTarget {
        public abstract String[] createTables();

        public static class Source extends BenchmarkTarget {
            @Override
            public String[] createTables() {
                return null;
            }
        }
        public static class Sink extends BenchmarkTarget {
            @Override
            public String[] createTables() {
                return null;
            }
        }
    }

    public static List<RunConfig> allRunConfigurations() {
        List<RunConfig> configs = new ArrayList<>();

        for (BenchmarkGoal goal : List.of(new BenchmarkGoal.Througput(), new BenchmarkGoal.Latency())) {
            for(BenchmarkTarget target : List.of(new BenchmarkTarget.Source(), new BenchmarkTarget.Sink())) {
                for (int cols : List.of(1, 2, 10)) {
                    for (int parallelism : List.of(1, 2, 8)) {
                        configs.add(new RunConfig(cols, parallelism, goal, target));
                    }
                }
            }
        }
        return configs;
    }
}
