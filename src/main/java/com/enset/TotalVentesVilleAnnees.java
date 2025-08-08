package com.enset;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.List;

public class TotalVentesVilleAnnees {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("VentesParVilleAnneeRDD").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        // (ville+année, prix)
        JavaPairRDD<String, Integer> ventesParVilleAnnee = lignes
                .mapToPair(ligne -> {
                    String[] champs = ligne.split(" ");
                    String date = champs[0];
                    String ville = champs[1];
                    int prix = Integer.parseInt(champs[3]);

                    String annee = date.split("-")[0];
                    String cle = ville + "_" + annee;

                    return new Tuple2<>(cle, prix);
                });

        JavaPairRDD<String, Integer> totalParVilleAnnee = ventesParVilleAnnee.reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> resultats = totalParVilleAnnee.collect();

        for (Tuple2<String, Integer> tuple : resultats) {

            String[] parts = tuple._1.split("_");
            String ville = parts[0];
            String annee = parts[1];
            System.out.println("Ville : " + ville + ", Année : " + annee + ", Total : " + tuple._2);
        }
        sc.close();
    }
}
