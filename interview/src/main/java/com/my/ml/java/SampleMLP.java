package com.my.ml.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.Imputer;
import org.apache.spark.ml.feature.ImputerModel;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamPair;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.my.ml.java.beans.HousingData;
import com.my.ml.java.helper.HousingMap;
import com.my.ml.java.helper.HousingMapRow;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.List;

import org.apache.spark.sql.catalyst.expressions.Expression;
import java.io.Serializable;

import java.util.function.Consumer;


public class SampleMLP implements Serializable {

	private static final long serialVersionUID = 53285393938417041L;


	public JavaRDD<HousingData> loadRddData(String project,JavaSparkContext ctx)
	{
		JavaRDD<HousingData> ret = null;
		JavaRDD<String> textfile = ctx.textFile("C:/stocks/learningSpark/spark-warehouse/housing.csv");
		System.out.println("Starting the map");
		ret = textfile.map(new HousingMap());
		System.out.println("Finishing the map  count is "+ret.count());
		ret.take(10).forEach(new Consumer<HousingData>() {
			@Override
			public void accept(HousingData t) {
				System.out.println( "data is "+t.toString());
			}});
		return ret;

	}
	public Dataset<HousingData> loadData(String project,SparkSession session) {
		Dataset<HousingData> data= null;
		if(project.equalsIgnoreCase("housing"))
		{
			Dataset<Row> textFile= session.read().csv("C:/stocks/learningSpark/spark-warehouse/housing.csv");
			textFile.show(10);
			data = textFile.map(new HousingMapRow(), Encoders.bean(HousingData.class));
		}
		return data;
	}

	private SparkSession getSession() {
		SparkConf conf  = new SparkConf(true)
				.setMaster("local[4]")
				.set("spark.executor.cores", "3")
				.set("spark.executor.memory","1g")
				//.set("spark.executor.instances", "4")
				.set("spark.executor.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
				//.set("spark.dynamicAllocation", "false")
				.set("spark.dynamicAllocation.maxExecutors", "4")
				.setAppName("SmartSparkLearner");
		SparkSession session = SparkSession.builder().config(conf)
				.appName("SmartSparkLearner")
				.getOrCreate();
		return session;
	}

	public Dataset<Row> performImputerFillValues(Dataset<Row> catg,String...cols )
	{
		Dataset<Row> ret = null;
		if(cols.length <1)
			throw new RuntimeException(" The passed value of columns are null");
		Imputer imputer = new Imputer();
		imputer = imputer.setInputCols(cols);
		String outCols[] = new String[cols.length];
		for(int i=0;i<cols.length;i++)
		{
			outCols[i]=cols[i]+"out";
		}
		imputer = imputer.setOutputCols(outCols);
		ImputerModel model = imputer.fit(catg);
		ret = model.transform(catg);
		System.out.println("From Filled");
		ret.show();
		return ret;
	}

	public Dataset<Row> encodeText(Dataset<Row> dataset, String col)
	{
		Dataset<Row> encoded = null;
		StringIndexer indexer = new StringIndexer().setInputCol(col).setOutputCol(col+"out");
		StringIndexerModel modlr = indexer.fit(dataset);
		encoded = modlr.transform(dataset);
		System.out.println("Encoded the text");
		encoded.show();
		OneHotEncoder enc = new OneHotEncoder().setInputCol(col+"out").setOutputCol(col+"vec");
		encoded = enc.transform(encoded);
		encoded.show();
		return encoded;
	}
	enum FEATURES {ROWID,ENCODE,BUCKETS,INCOMECATEGORY,CUSTOMATTR};
	/** Performs the pipe features */
	public void performPipelines(Dataset<Row> rawData,FEATURES[] pipeFeatures) {
		Pipeline pipes = new Pipeline();
		PipelineStage[] value= new PipelineStage[FEATURES.values().length];
		for(FEATURES features:pipeFeatures)
		{
			switch(features) {
			case ROWID:{

			}
			}
		}
		pipes.setStages(value);
	}
	public Dataset<Row> createBuckets(Dataset<Row> dataset)
	{
		
		System.out.println("From Creating buckets");
		double[] splits = {Double.NEGATIVE_INFINITY, -.05, 0.0, .05, Double.POSITIVE_INFINITY};
		System.out.println("Splits are ");
		for(double split:splits) {
			System.out.print("\t"+split);
		}
		Bucketizer buck = new Bucketizer().setInputCol("incomeCategory").setOutputCol("incomeCategoryType").setSplits(splits);
		Dataset<Row> anotherCatg = buck.transform(dataset);
		anotherCatg.show();
		System.out.println("Showingcategory type");
		anotherCatg.select("incomeCategoryType").distinct().show();
		return anotherCatg;
	}
	
	
	public void test() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession sess = getSession();
		Dataset<HousingData> loads = loadData("housing",sess);
		//loadRddData("housing",JavaSparkContext.fromSparkContext(getSession().sparkContext()));
		Dataset<HousingData>[] testAndReal = loads.randomSplit(new double[]{0.2,0.8});
		Dataset<Row> testset = transformHousingData(testAndReal[0]);
		System.out.println("Test data count is"+testAndReal[0].count());
		
	 	Dataset<Row> trainset = transformHousingData(testAndReal[1]);
	 	System.out.println("Train data is"+testAndReal[1].count());
		
	 	//trainsetWithCatg.na().fill(1);// value to fill empty columns.
  		//	Correlation.corr(trainsetWithCatg, "incomeCategory").show(20);
		double corr = trainset.stat().corr("median_house_value", "median_income");
 		System.out.println("Correlation between \"median_house_value\", \"median_income\" is"+corr);
		//performRegression(sess);
 	//	testset = testset.drop("median_house_value");
		performRegression(trainset,testset);
		performDecisionTrees(trainset,testset);
		//performDecisionTrees(transformHousingData(loads));
		//performpredictions(values);
		System.out.println("Completed spark");
		sess.stop();
		sess.close();
	}
	
	private void performDecisionTrees(Dataset<Row> trainset, Dataset<Row> testset) {
		
		System.out.println("Performing Decision Tree Regression");
 		VectorIndexer indx = new VectorIndexer().setMaxCategories(5).setInputCol("features").setOutputCol("indexedFeatures");
	 	DecisionTreeRegressor dtr = new DecisionTreeRegressor().setFeaturesCol("indexedFeatures").setLabelCol("median_house_value");
		Pipeline stages = new Pipeline().setStages( new PipelineStage[] {indx,dtr});
		PipelineModel model = stages.fit(trainset);
		Dataset<Row> predictedSet = model.transform(testset);
		predictedSet.show();
	}
	
	private Dataset<Row> transformHousingData(Dataset<?> trainsetWithCatg1)
	{
		Dataset<Row> ret = null;
		ret = trainsetWithCatg1.withColumn("rowId",functions.monotonicallyIncreasingId());
		ret=  ret.withColumn("incomeCategory",functions.ceil(functions.expr("median_income / 1.5")));
		ret = ret.withColumn("roomsPerHousehold", functions.expr("total_rooms / households"));// roomsper household
		ret = ret.withColumn("bedroomsPerRooms", functions.expr("total_bedrooms / total_rooms"));
		ret = ret.withColumn("popPerhouseholds", functions.expr("population/households"));
		ret = createBuckets(ret); 
		ret=encodeText(ret, "ocean_proximity");
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] {"longitude","latitude","housing_median_age","incomeCategory","rowId","ocean_proximityvec"})
				.setOutputCol("features");
		ret = assembler.transform(ret);
 		ret.show();
		return ret;
	}
	private void performRegression(Dataset<Row> values,Dataset<Row> test)// SparkSession sparkSession)
	{
		//Dataset<Row> numerics=values.drop("ocean_proximity");
		//		Dataset<Row> values = sparkSession.read().format("libsvm")
		//				.load("C:\\spark\\spark-2.2.1-bin-hadoop2.7\\data\\mllib\\sample_linear_regression_data.txt");
		System.out.println(" Inside the Multivariable Regression");
		LinearRegression lr = new LinearRegression()
				.setMaxIter(1000)
				.setRegParam(0.3)
				.setStandardization(true)
	 			.setElasticNetParam(0.8)
				.setLabelCol("median_house_value");
		lr.extractParamMap().toList().forEach(new Consumer<ParamPair<?>>(){

			@Override
			public void accept(ParamPair<?> t) {
				 System.out.println("t.productPrefix()+t.value()"+t.productPrefix()+t.value());
			}});;
 		LinearRegressionModel lrModel=lr.fit(values);
 		lrModel.transform(test).show();
		//		
		//		RDD<LabeledPoint> labels = lr.extractLabeledPoints(numericsVect);
		//		System.out.println(labels.first().toString());
		//	LinearRegressionModel lrml = lr.fit(numerics);
		printRegressionModelSummary(lrModel);
	}
	
	private void printRegressionModelSummary(LinearRegressionModel lrModel) {
		// Print the coefficients and intercept for linear regression.
		System.out.println("Coefficients: "
		  + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
 		// Summarize the model over the training set and print out some metrics.
		LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
		System.out.println("numIterations: " + trainingSummary.totalIterations());
		System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
		trainingSummary.residuals().show();
		System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("r2: " + trainingSummary.r2());
	}
	
	private void performpredictions(Dataset<Row> values, Dataset<Row> testModels) {
		java.util.List<Row> stats = values.describe().collectAsList();
		for(int i=0;i<stats.size();i++)
		{
			for(int j=0;j<stats.get(i).size();j++)
			{
				System.out.println(stats.get(i).getString(j));
			}
		}
		VectorAssembler vectorise = new VectorAssembler();
		Tuple2<String,String>[] vals = values.dtypes();
		for(int i=0;i<vals.length;i++) {
			System.out.println(vals[i]._1+"/t"+vals[i]._2);
		}
		Dataset<Row> numerics=values.drop("ocean_proximity");
		vectorise = vectorise.setInputCols(numerics.columns());
		vectorise = vectorise.setOutputCol("features");
		//	StructType vect = vectorise.transformSchema(values.schema());
		//	System.out.println(vect.toString());
		LogisticRegression lr = new LogisticRegression();
 		lr = lr.setRegParam(.5)
 				.setMaxIter(1000).setLabelCol("incomeCategoryType")
				.setElasticNetParam(.8).setFeaturesCol("ocean_proximityvec").setStandardization(true);
		LogisticRegressionModel lrml = lr.fit(numerics);
		Dataset<Row> model = lrml.transform(testModels);
		System.out.println("Multinomial coefficients: " + lrml.coefficientMatrix()
		+ "\nMultinomial intercepts: " + lrml.intercept());


	}
	// 6699006833 3231111305#
	private void testCorrelation(Dataset testsetWithCatg, SparkSession sess) {
		Vector v;
		DataType vsg = VectorUDT.fromJson(testsetWithCatg.schema().json());
		StructType schema = new StructType(new StructField[]{
				new StructField("households",DataTypes.IntegerType , false, Metadata.empty()),
				new StructField("housing_median_age",DataTypes.DoubleType , false, Metadata.empty()),
				new StructField("longitude",DataTypes.DoubleType , false, Metadata.empty()),
				new StructField("latitude",DataTypes.DoubleType , false, Metadata.empty()),
				new StructField("median_house_value",DataTypes.DoubleType , false, Metadata.empty()),
				new StructField("median_income",DataTypes.DoubleType , false, Metadata.empty()),
				new StructField("ocean_proximity",DataTypes.StringType , false, Metadata.empty()),
				new StructField("population",DataTypes.IntegerType , false, Metadata.empty()),
				new StructField("total_bedrooms",DataTypes.IntegerType , false, Metadata.empty()),
				new StructField("total_rooms",DataTypes.IntegerType , false, Metadata.empty()),
				new StructField("incomeCategory",DataTypes.DoubleType , false, Metadata.empty()),
				new StructField("rowId",DataTypes.LongType , false, Metadata.empty()),
		});
		//"longitude","latitude","housing_median_age","median_house_value"
		//		 |-- households: integer (nullable = true)
		//		 |-- housing_median_age: double (nullable = true)
		//		 |-- intitude: double (nullable = true)
		//		 |-- latitude: double (nullable = true)
		//		 |-- median_house_value: double (nullable = true)
		//		 |-- median_income: double (nullable = true)
		//		 |-- ocean_proximity: string (nullable = true)
		//		 |-- population: integer (nullable = true)
		//		 |-- total_bedrooms: integer (nullable = true)
		//		 |-- total_rooms: integer (nullable = true)
		//		 |-- incomeCategory: long (nullable = true)
		//		 |-- rowId: long (nullable = false)

		SQLTransformer sqlt = new SQLTransformer().setStatement("Select *  FROM __THIS__");

		Dataset<Row> testsetWithCatgCorr = sess.createDataFrame(testsetWithCatg.rdd(), schema);
		Correlation.corr(testsetWithCatg, "median_house_value").show(20);

	}
	public static void main(String args[])
	{
		SampleMLP mlp = new SampleMLP();
		mlp.test();
	}


}




