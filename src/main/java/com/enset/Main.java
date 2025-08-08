package com.enset;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class Main{

    public static void main(String[] args) {

        //Pour configurer notre application, nous utilisons setMaster afin de préciser où elle sera déployée.
        SparkConf conf = new SparkConf().setAppName("VentesParVilleRDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier texte dans un RDD
        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        JavaPairRDD<String, Integer> ventesVille = lignes
                .mapToPair(ligne -> {
                    String[] champs = ligne.split(" ");
                    String ville = champs[1];
                    int prix = Integer.parseInt(champs[3]);
                    return new Tuple2<>(ville, prix);
                });

        // Regrouper et additionner par ville
        JavaPairRDD<String, Integer> totalParVille = ventesVille.reduceByKey(Integer::sum);

        // Afficher les résultats
        for (Tuple2<String, Integer> tuple : totalParVille.collect()) {
            System.out.println("Ville : " + tuple._1 + ", Total : " + tuple._2);
        }

        sc.close();
    }
}