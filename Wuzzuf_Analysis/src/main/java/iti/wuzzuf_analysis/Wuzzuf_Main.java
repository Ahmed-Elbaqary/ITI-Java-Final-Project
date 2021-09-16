/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package iti.wuzzuf_analysis;

import java.util.List;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;

/**
 *
 * @author ahmed
 */
public class Wuzzuf_Main {
    
    @SuppressWarnings("empty-statement")
    public static void main(String[] args) 
    {
        // ========================= Step (1) ===========================================
        // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder ().appName ("Spark Wuzzuf Analysis").master ("local[2]")
                .getOrCreate ();
        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");

        
        // ========================= Step (2) ===========================================
        // Print Schema to see column names, types and other metadata
        csvDataFrame.printSchema ();
        csvDataFrame.describe().show();
        
        
        // ========================= Step (3) ===========================================
        System.out.println(" =========== Before Cleaning Data =========== "+csvDataFrame.count());
        final Dataset<Row> cleanDataFrame = csvDataFrame.dropDuplicates().drop();
        System.out.println(" =========== After Cleaning Data =========== "+cleanDataFrame.count());
        
        
        
        // ========================= Step (4) ===========================================
        cleanDataFrame.createOrReplaceTempView ("Jobs_Of_Company");
        final Dataset<Row> JobsOfCompany = sparkSession
                .sql ("SELECT COUNT(Title) jobs, Company FROM Jobs_Of_Company GROUP BY Company "
                        + "ORDER BY jobs DESC LIMIT 10");
        JobsOfCompany.show ();
        
        
        // ========================= Step (5) ===========================================
        List<Float> company_jobs_Num = JobsOfCompany.select("jobs").as(Encoders.FLOAT()).collectAsList();
        List<String> company = JobsOfCompany.select("Company").as(Encoders.STRING()).collectAsList();
        
        PieChart chart = new PieChartBuilder().width(800).height(800).title("jobs_for_each_company").build();
        for (int i = 0 ; i < company.size(); ++i)
        {   // accessing each element of array
            chart.addSeries (company.get(i), company_jobs_Num.get(i));
        };
        new SwingWrapper(chart).displayChart(); //display
        
        
        
        // ========================= Step (6) ===========================================
        cleanDataFrame.createOrReplaceTempView ("Most_Job_Title");
        final Dataset<Row> MostJobTitle = sparkSession
                .sql ("SELECT COUNT(Title) Job_Numbers, Title FROM Most_Job_Title GROUP BY Title "
                        + "ORDER BY Job_Numbers DESC LIMIT 10" );
        MostJobTitle.show ();
        
        // ========================= Step (7) ===========================================
        List<Float> jobs_Num = MostJobTitle.select("Job_Numbers").as(Encoders.FLOAT()).collectAsList();
        List<String> Position = MostJobTitle.select("Title").as(Encoders.STRING()).collectAsList();
        
        CategoryChart Titles_chart = new CategoryChartBuilder ().width (1024).height (768)
                .title ("Jobs Histogram").xAxisTitle ("Position").yAxisTitle("jobs_Num").build ();
        // Customize Chart
        Titles_chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        Titles_chart.getStyler ().setHasAnnotations (true);
        Titles_chart.getStyler ().setStacked (true);
        // Series
        Titles_chart.addSeries ("Most Popular Job Titles", Position, jobs_Num);
        // Show it
        new SwingWrapper (Titles_chart).displayChart ();
        
        
        // ========================= Step (8) ===========================================
        cleanDataFrame.createOrReplaceTempView ("Popular_Areas");
        final Dataset<Row> MostPopularAreas = sparkSession
                .sql ("SELECT COUNT(Title) Jobs_In_Area, Location FROM Popular_Areas GROUP BY Location "
                        + "ORDER BY Jobs_In_Area DESC LIMIT 10");
        MostPopularAreas.show (); 
        
        
        // ========================= Step (9) ===========================================
        List<Float> Jobs_In_Area = MostPopularAreas.select("Jobs_In_Area").as(Encoders.FLOAT()).collectAsList();
        List<String> Area = MostPopularAreas.select("Location").as(Encoders.STRING()).collectAsList();
        
        CategoryChart area_chart = new CategoryChartBuilder ().width (1024).height (768).title ("Jobs Histogram").xAxisTitle ("Position").yAxisTitle
        ("jobs_Num").build ();
        // Customize Chart
        area_chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        area_chart.getStyler ().setHasAnnotations (true);
        area_chart.getStyler ().setStacked (true);
        // Series
        area_chart.addSeries ("Most Popular Jobs Areas", Area, Jobs_In_Area);
        // Show it
        new SwingWrapper (area_chart).displayChart ();
        
        
        // ========================= Step (10) ===========================================
        cleanDataFrame.createOrReplaceTempView ("Most_Wanted_Skill");
        final Dataset<Row> MostWantedSkill = sparkSession.sql("select col , count(col) as No_of_skills  FROM  (SELECT explode(split(skills, ',')) AS `col` FROM Most_Wanted_Skill) "
                + "group by (col) order by (No_of_skills)  desc ");
        MostWantedSkill.show ();
    }
}
