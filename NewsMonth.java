
//Set appropriate package name

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.io.*; 


import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;




public class NewsMonth {

	public static void main(String[] args) throws Exception {
		
		
		String inputPath="/home/panda/Downloads/newsdata"; 
		
		
		String outputPath="/home/panda/Downloads/outputNewsMonth"; 
		
		SparkSession sparkSession = SparkSession.builder()
				.appName("Month wise news articles")		
				.master("local")								
				.config("spark.sql.shuffle.partitions","2")		
				.getOrCreate();
		
		
		Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath); 
		
		File filePositive = new File("/home/panda/Downloads/positive-words.txt");
		File fileNegative = new File("/home/panda/Downloads/negative-words.txt");
		File fileEntity = new File("/home/panda/Downloads/entities.txt");
		
		BufferedReader pstv = new BufferedReader(new FileReader(filePositive));
		BufferedReader ngtv = new BufferedReader(new FileReader(fileNegative));
		BufferedReader enty = new BufferedReader(new FileReader(fileEntity));
		
		HashSet<String> positive = new HashSet<String>();
		HashSet<String> negative = new HashSet<String>();
		HashSet<String> entity = new HashSet<String>();
		
		String temp1;
		String temp2;
		String temp3;
		
		while((temp1 = pstv.readLine()) != null) {
			positive.add(temp1);
		}
		
		while((temp2 = ngtv.readLine()) != null) {
			negative.add(temp2);
		}
		
		while((temp3 = enty.readLine()) != null) {
			entity.add(temp3);
		}
		
		
		Dataset<String> yearMonthDataset=inputDataset.flatMap(new FlatMapFunction<Row, String>(){
			public Iterator<String> call(Row row) throws Exception {
			
				String source_n=((String)row.getAs("source_name"));
				String yearMonthPublished=((String)row.getAs("date_published")).substring(0, 7);
				String article_b=((String)row.getAs("article_body"));
				article_b = article_b.toLowerCase().replaceAll("[^A-Za-z]", " "); 
				article_b = article_b.replaceAll("( )+", " ");   
				article_b = article_b.trim(); 
				List<String> wordList = Arrays.asList(article_b.split(" ")); 
				List<String> myList = new ArrayList<String>();
				
				for (int i = 0; i < wordList.size(); i++) {
					String curr = wordList.get(i);
					if(entity.contains(curr)) {
						
						int flag = 0;
						for (int j = i-5; j<= i+5; j++) {
							String c = "";
							if(j >= 0 && j<wordList.size()) {
								if(positive.contains(wordList.get(j))) {
									c = "1";
									myList.add(source_n + " "+ yearMonthPublished+" " + wordList.get(i) + " " +"1") ;
									flag++;
									
								}
								if(negative.contains(wordList.get(j))) {
									c = "-1";
									myList.add(source_n + " "+ yearMonthPublished+" " + wordList.get(i) + " " +"-1") ;
									flag++;
									
								}
								
								
							}
						}
						if (flag == 0) {
							myList.add(source_n + " "+ yearMonthPublished+" " + wordList.get(i) + " " + "0") ;
						}
					}
				}
				
				return myList.iterator();	  
			}
			
		}, Encoders.STRING());
		
		
		Dataset<Row> count=yearMonthDataset.groupBy("value").count().as("sentiment_count");
		List<Row> list_rows = count.collectAsList();
		
		
		
				
				
				
				
		
		List<Row> allRows = new ArrayList<Row>();
//		System.out.println(list_rows);
		for(int i = 0; i< list_rows.size(); i++) {
			String firstpart = list_rows.get(i).get(0).toString();
			int secondpart = Integer.parseInt(list_rows.get(i).get(1).toString());
			
			List<String> temp_word = Arrays.asList(firstpart.split(" "));
			
			Row r_temp = RowFactory.create(temp_word.get(0), temp_word.get(1), temp_word.get(2), Integer.parseInt(temp_word.get(3)), secondpart);
			allRows.add(r_temp);
		}
		
		
		StructType schema = DataTypes.createStructType(
				(org.apache.spark.sql.types.StructField[]) new StructField[] {
						DataTypes.createStructField("source_name", DataTypes.StringType, false),
						DataTypes.createStructField("year_month", DataTypes.StringType, false),
						DataTypes.createStructField("entity", DataTypes.StringType, false),
						DataTypes.createStructField("sentiment", DataTypes.IntegerType, false),
						DataTypes.createStructField("count", DataTypes.IntegerType, false),
						
						
						
					
				});
				
				
		Dataset<Row> final_data = sparkSession.sqlContext().createDataFrame(allRows, schema);
		
		
		Dataset<Row> final_data_aggregated = final_data.groupBy("source_name", "year_month", "entity").agg(functions.sum(functions.col("count").multiply(functions.col("sentiment")))
				.as("overall_sentiment"), functions.sum("count").as("overall_support")).where(functions.sum("count").$greater$eq(5));
		
		
		Dataset<Row> result = final_data_aggregated.select("source_name", "year_month", "entity", "overall_sentiment").orderBy(functions.abs(functions.col("overall_sentiment")).desc());
		
		
		
		

		result.toJavaRDD().saveAsTextFile(outputPath);	
		
	}
	
}