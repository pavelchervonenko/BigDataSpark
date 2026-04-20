package org.example.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import org.example.config.AppConfig;

import java.util.Properties;

public class TransformJob {

    public static void run() {
        SparkSession spark = SparkSession.builder()
                .appName("TransformJob")
                .getOrCreate();

        Properties pgProps = new Properties();
        pgProps.setProperty("user", AppConfig.PG_USER);
        pgProps.setProperty("password", AppConfig.PG_PASSWORD);
        pgProps.setProperty("driver", AppConfig.PG_DRIVER);

        // ЧИТАЕМ ИСХОДНУЮ ТАБЛИЦУ
        Dataset<Row> mockData = spark.read()
                .jdbc(AppConfig.PG_URL,"mock_data", pgProps);

        // ТАБЛИЦЫ ИЗМЕРЕНИЙ
        Dataset<Row> dimPet = mockData
                .select("customer_pet_type", "customer_pet_breed")
                .distinct()
                .toDF("pet_type", "pet_breed")
                .withColumn("pet_id", functions.monotonically_increasing_id().plus(1));

        Dataset<Row> dimCustomerRaw = mockData
                .select("sale_customer_id", "customer_first_name", "customer_last_name",
                        "customer_age", "customer_email", "customer_country",
                        "customer_postal_code", "customer_pet_type", "customer_pet_breed")
                .distinct();

        Dataset<Row> dimCustomer = dimCustomerRaw
                .join(dimPet,
                        dimCustomerRaw.col("customer_pet_type").equalTo(dimPet.col("pet_type"))
                                .and(dimCustomerRaw.col("customer_pet_breed").equalTo(dimPet.col("pet_breed"))),
                        "left")
                .select(
                        dimCustomerRaw.col("sale_customer_id").as("customer_id"),
                        dimCustomerRaw.col("customer_first_name").as("first_name"),
                        dimCustomerRaw.col("customer_last_name").as("last_name"),
                        dimCustomerRaw.col("customer_age").as("age"),
                        dimCustomerRaw.col("customer_email").as("email"),
                        dimCustomerRaw.col("customer_country").as("country"),
                        dimCustomerRaw.col("customer_postal_code").as("postal_code"),
                        dimPet.col("pet_id")
                )
                .dropDuplicates("customer_id");

        Dataset<Row> dimSeller = mockData
                .select("sale_seller_id", "seller_first_name", "seller_last_name", "seller_email",
                        "seller_country", "seller_postal_code")
                .dropDuplicates("sale_seller_id")
                .toDF("seller_id", "first_name", "last_name", "email", "country", "postal_code");

        Dataset<Row> dimProduct = mockData
                .select("sale_product_id", "product_name", "product_category",
                        "product_price", "product_quantity", "product_weight",
                        "product_color", "product_size", "product_brand",
                        "product_material", "product_description", "product_rating",
                        "product_reviews", "product_release_date", "product_expiry_date",
                        "pet_category")
                .dropDuplicates("sale_product_id")
                .toDF("product_id", "name", "category", "price", "quantity",
                        "weight", "color", "size", "brand", "material",
                        "description", "rating", "reviews", "release_date",
                        "expiry_date", "pet_category");

        Dataset<Row> dimStore = mockData
                .select("store_name", "store_location", "store_city",
                        "store_state", "store_country", "store_phone", "store_email")
                .dropDuplicates("store_name")
                .toDF("name", "location", "city", "state", "country", "phone", "email");

        Dataset<Row> dimSupplier = mockData
                .select("supplier_name", "supplier_contact", "supplier_email",
                        "supplier_phone", "supplier_address", "supplier_city",
                        "supplier_country")
                .dropDuplicates("supplier_name")
                .toDF("name", "contact", "email", "phone",
                        "address", "city", "country");

        // ТАБЛИЦА ФАКТОВ
        Dataset<Row> factSale = mockData
                .select("sale_customer_id", "sale_seller_id", "sale_product_id",
                        "store_name", "supplier_name", "sale_date",
                        "sale_quantity", "sale_total_price")
                .toDF("customer_id", "seller_id", "product_id",
                        "store_name", "supplier_name", "sale_date",
                        "sale_quantity", "sale_total_price");

        // ЗАПИСЫВАЕМ В PostgreSQL
        writeToPostgres(dimPet, "dim_pet", pgProps);
        writeToPostgres(dimCustomer, "dim_customer", pgProps);
        writeToPostgres(dimSeller, "dim_seller", pgProps);
        writeToPostgres(dimProduct, "dim_product", pgProps);
        writeToPostgres(dimStore, "dim_store", pgProps);
        writeToPostgres(dimSupplier, "dim_supplier", pgProps);
        writeToPostgres(factSale, "fact_sale", pgProps);

        spark.stop();
    }

    private static void writeToPostgres(Dataset<Row> df, String table, Properties props) {
        df.write()
                .mode("overwrite")
                .jdbc(AppConfig.PG_URL, table, props);
    }
}

