package org.example.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import org.example.config.AppConfig;

import java.util.Properties;

public class ClickHouseJob {
    public static void run() {
        SparkSession spark = SparkSession.builder()
                .appName("ClickHouseReportJob")
                .getOrCreate();

        // ЧТЕНИЕ ИЗ PostgreSQL
        Properties pgProps = new Properties();
        pgProps.setProperty("user", AppConfig.PG_USER);
        pgProps.setProperty("password", AppConfig.PG_PASSWORD);
        pgProps.setProperty("driver", AppConfig.PG_DRIVER);

        Dataset<Row> factSale = spark.read().jdbc(AppConfig.PG_URL, "fact_sale", pgProps);
        Dataset<Row> dimProduct = spark.read().jdbc(AppConfig.PG_URL, "dim_product", pgProps);
        Dataset<Row> dimCustomer = spark.read().jdbc(AppConfig.PG_URL, "dim_customer", pgProps);
        Dataset<Row> dimStore = spark.read().jdbc(AppConfig.PG_URL, "dim_store", pgProps);
        Dataset<Row> dimSupplier = spark.read().jdbc(AppConfig.PG_URL, "dim_supplier", pgProps);

        // ВИТРИНА ПО ПРОДУКТАМ
        Dataset<Row> reportProducts = factSale
                .join(dimProduct, factSale.col("product_id").equalTo(dimProduct.col("product_id")))
                .groupBy(
                        dimProduct.col("product_id"),
                        dimProduct.col("name"),
                        dimProduct.col("category"),
                        dimProduct.col("rating"),
                        dimProduct.col("reviews")
                )
                .agg(
                        functions.sum("sale_quantity").as("total_quantity"),
                        functions.sum("sale_total_price").as("total_revenue"),
                        functions.count("*").as("sales_count")
                )
                .orderBy(functions.desc("total_quantity"));

        // ПО КЛИЕНТАМ
        Dataset<Row> reportCustomers = factSale
                .join(dimCustomer, factSale.col("customer_id").equalTo(dimCustomer.col("customer_id")))
                .groupBy(
                        dimCustomer.col("customer_id"),
                        dimCustomer.col("first_name"),
                        dimCustomer.col("last_name"),
                        dimCustomer.col("country")
                )
                .agg(
                        functions.sum("sale_total_price").as("total_spent"),
                        functions.avg("sale_total_price").as("avg_check"),
                        functions.count("*").as("purchases_count")
                )
                .orderBy(functions.desc("total_spent"));

        // ПО ВРЕМЕНИ
        Dataset<Row> reportTime = factSale
                .withColumn("sale_date_parsed",
                        functions.to_date(factSale.col("sale_date"), "M/d/yyyy"))
                .withColumn("year",
                        functions.year(functions.col("sale_date_parsed")))
                .withColumn("month",
                        functions.month(functions.col("sale_date_parsed")))
                .groupBy("year", "month")
                .agg(
                        functions.sum("sale_total_price").as("total_revenue"),
                        functions.count("*").as("sales_count"),
                        functions.avg("sale_quantity").as("avg_order_size")
                )
                .orderBy("year", "month");

        // ПО МАГАЗИНАМ
        Dataset<Row> reportStores = factSale
                .join(dimStore, factSale.col("store_name").equalTo(dimStore.col("name")))
                .groupBy(
                        dimStore.col("name"),
                        dimStore.col("city"),
                        dimStore.col("country")
                )
                .agg(
                        functions.sum("sale_total_price").as("total_revenue"),
                        functions.avg("sale_total_price").as("avg_check"),
                        functions.count("*").as("sales_count")
                )
                .orderBy(functions.desc("total_revenue"));

        // ПО ПОСТАВЩИКАМ
        Dataset<Row> reportSuppliers = factSale
                .join(dimSupplier, factSale.col("supplier_name").equalTo(dimSupplier.col("name")))
                .join(dimProduct, factSale.col("product_id").equalTo(dimProduct.col("product_id")))
                .groupBy(
                        dimSupplier.col("name"),
                        dimSupplier.col("country")
                )
                .agg(
                        functions.sum("sale_total_price").as("total_revenue"),
                        functions.avg(dimProduct.col("price")).as("avg_product_price"),
                        functions.count("*").as("sales_count")
                )
                .orderBy(functions.desc("total_revenue"));

        // КАЧЕСТВО ПРОДУКЦИИ
        Dataset<Row> reportQuality = factSale
                .join(dimProduct, factSale.col("product_id").equalTo(dimProduct.col("product_id")))
                .groupBy(
                        dimProduct.col("product_id"),
                        dimProduct.col("name"),
                        dimProduct.col("category"),
                        dimProduct.col("rating"),
                        dimProduct.col("reviews")
                )
                .agg(
                        functions.sum("sale_quantity").as("total_sold"),
                        functions.count("*").as("sales_count")
                )
                .orderBy(functions.desc("rating"));

        // ЗАПИСЬ В ClickHouse
        writeToClickHouse(reportProducts, "report_products");
        writeToClickHouse(reportCustomers, "report_customers");
        writeToClickHouse(reportTime, "report_time");
        writeToClickHouse(reportStores, "report_stores");
        writeToClickHouse(reportSuppliers, "report_suppliers");
        writeToClickHouse(reportQuality, "report_quality");

        spark.stop();
    }

    private static void writeToClickHouse(Dataset<Row> df, String table) {
        Properties chProps = new Properties();
        chProps.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver");
        chProps.setProperty("user", AppConfig.CH_USER);
        chProps.setProperty("password", AppConfig.CH_PASSWORD);

        df.write()
                .mode("overwrite")
                .jdbc(AppConfig.CH_URL, table, chProps);
    }
}
