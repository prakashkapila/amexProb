package com.durgaveg.learningSpark.cassandra;
import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.durgaveg.learningSpark.cassandra.beans.*;

import static  com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class SparkCassandraConnector implements Serializable {
     private JavaSparkContext sc = null;
  
    private void run() {
    	SparkConf conf = new SparkConf();
    	conf.setAppName("Spark To Cassandra");
    	conf.setMaster("local[4]");
    	conf.set("spark.cassandra.connection.host", "localhost");
    	sc = new JavaSparkContext();
        generateData(sc);
        compute(sc);
        showResults(sc);
        sc.stop();
    }

	private void showResults(JavaSparkContext sc) {
		// TODO Auto-generated method stub
		
	}

	private void compute(JavaSparkContext sc) {
		// TODO Auto-generated method stub
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		try (Session session = connector.openSession()) {
		    session.execute("DROP KEYSPACE IF EXISTS java_api");
		    session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		    session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
		    session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
		    session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
		}// Prepare the products hierarchy
        List<Product> products = Arrays.asList(
                new Product(0, "All products", Collections.<Integer>emptyList()),
                new Product(1, "Product A", Arrays.asList(0)),
                new Product(4, "Product A1", Arrays.asList(0, 1)),
                new Product(5, "Product A2", Arrays.asList(0, 1)),
                new Product(2, "Product B", Arrays.asList(0)),
                new Product(6, "Product B1", Arrays.asList(0, 2)),
                new Product(7, "Product B2", Arrays.asList(0, 2)),
                new Product(3, "Product C", Arrays.asList(0)),
                new Product(8, "Product C1", Arrays.asList(0, 3)),
                new Product(9, "Product C2", Arrays.asList(0, 3))
        );
        JavaRDD<Product> productRdd = sc.parallelize(products);
        ColumnMapper mapper = null;
        RowWriterFactory<Product> fact = mapToRow(Product.class, mapper);
      //  javaFunctions(productRdd).writerBuilder("java_api", "products", rowWriterFactory)
	}

	private void generateData(JavaSparkContext sc) {
		// TODO Auto-generated method stub
		
	}
}
