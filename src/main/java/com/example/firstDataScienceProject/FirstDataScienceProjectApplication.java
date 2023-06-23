package com.example.firstDataScienceProject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.Plot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BoxAndWhiskerRenderer;
import org.jfree.chart.renderer.xy.XYBoxAndWhiskerRenderer;
import org.jfree.data.statistics.BoxAndWhiskerItem;
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset;
import org.jfree.data.statistics.DefaultBoxAndWhiskerXYDataset;
import org.jfree.data.statistics.HistogramDataset;

import java.awt.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FirstDataScienceProjectApplication {

    public static void main(String[] args) {
        // Cria uma instância do SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Exemplo de leitura de arquivo CSV")
                .master("local[*]")
                .getOrCreate();

        // Lê o arquivo CSV como um DataFrame
        Dataset<Row> dataframe = spark.read()
                .format("csv")
                .option("header", "true") // Se o arquivo possui cabeçalho
                .option("inferSchema", "true") // Inferir o schema dos dados
                .load("src/main/resources/telecom_users.csv");

        Dataset<Row> cleanedData = cleanedData = dataframe.drop("Codigo");
        cleanedData = cleanedData.na().drop();
        cleanedData = cleanedData.dropDuplicates();
        cleanedData = cleanedData.drop("_c0");

        cleanedData.show();
        System.out.println("CleanedData rows: " + cleanedData.count());

        // Agrupamento por "TipoContrato" e média da coluna "ValorMensal"
        Dataset<Row> groupedData = cleanedData.groupBy("TipoContrato").mean("ValorMensal");

        // Exibição dos resultados
        groupedData.show();

        // Coleta os valores da coluna em uma lista
        List<Double> values = cleanedData.select("MesesComoCliente").as(Encoders.DOUBLE()).collectAsList();

        // Cria um conjunto de dados para o histograma
        HistogramDataset dataset = new HistogramDataset();
        double[] data = new double[values.size()];

        for (int i = 0; i < values.size(); i++) {
            data[i] = values.get(i);
        }

        dataset.addSeries("Histograma", data, 10); // 5 é o número de bins (intervalos)

        // Cria o gráfico de histograma
        JFreeChart chart = ChartFactory.createHistogram("Histograma", "Valores", "Frequência",
                dataset, PlotOrientation.VERTICAL, true, true, false);

        // Define as cores do gráfico
        chart.setBackgroundPaint(Color.white);
        chart.getPlot().setBackgroundPaint(Color.lightGray);

        // Cria a janela do gráfico
        ChartFrame frame = new ChartFrame("Histograma", chart);
        frame.pack();
        frame.setVisible(true);


        // Cria um conjunto de dados para o Box Plot
        DefaultBoxAndWhiskerCategoryDataset datasetBoxPlot = new DefaultBoxAndWhiskerCategoryDataset();
        datasetBoxPlot.add(values, "Valores", "Box Plot");

        // Cria o gráfico de Box Plot
        JFreeChart chartBoxPlot = ChartFactory.createBoxAndWhiskerChart(
                "Box Plot", // Título do gráfico
                "Valores", // Rótulo do eixo X
                "Frequência", // Rótulo do eixo Y
                datasetBoxPlot, // Conjunto de dados
                true // Exibir legenda
        );

        // Define as cores do gráfico
        chartBoxPlot.setBackgroundPaint(Color.white);
        chartBoxPlot.getPlot().setBackgroundPaint(Color.lightGray);

        CategoryPlot plot = (CategoryPlot) chartBoxPlot.getPlot();
        BoxAndWhiskerRenderer renderer = (BoxAndWhiskerRenderer) plot.getRenderer();
        renderer.setMaximumBarWidth(0.2);

        // Cria a janela do gráfico
        ChartFrame frameBoxPlot = new ChartFrame("Box Plot", chartBoxPlot);
        frameBoxPlot.pack();
        frameBoxPlot.setVisible(true);

        // Encerra a sessão do Spark
        spark.stop();
    }
}
