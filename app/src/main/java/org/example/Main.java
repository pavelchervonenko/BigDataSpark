package org.example;

import org.example.jobs.CassandraJob;
import org.example.jobs.ClickHouseJob;
import org.example.jobs.MongoJob;
import org.example.jobs.TransformJob;

public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: spark-submit app.jar <job>");
            System.out.println("Jobs: transform, clickhouse, cassandra, mongo");
            return;
        }

        switch (args[0]) {
            case "transform":
                TransformJob.run();
                break;
            case "clickhouse":
                ClickHouseJob.run();
                break;
            case "cassandra":
                //CassandraJob.run();
                break;
            case "mongo":
                //MongoJob.run();
                break;
            default:
                System.out.println("Unknown job: " + args[0]);
        }
    }
}
