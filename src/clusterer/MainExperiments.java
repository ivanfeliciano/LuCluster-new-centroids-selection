/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clusterer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ivan
 */
public class MainExperiments {
    public static void main(String[] args) {
        try {
            PrintStream out;
            
            if (args.length == 0) {
                args = new String[1];
                System.out.println("Usage: java FastKMedoidsClusterer <prop-file>");
                args[0] = "init_0.properties";
            }
            
            LuceneClusterer algorithms [];
            algorithms = new LuceneClusterer[5];
            algorithms[0] = new FPACWithMCentroids(args[0]);
            algorithms[1] = new KMedoidsClusterer(args[0]);
            algorithms[2] = new FastKMedoidsClusterer_TrueCentroid(args[0]);
            algorithms[3] = new KMeansClusterer(args[0]);
            algorithms[4] = new ScalableKMeans(args[0]);
            
            for (int algoIterator = 0; algoIterator < 5; algoIterator++) {
                out = new PrintStream(new FileOutputStream("logs_tweets_algorithm" + algoIterator + ".txt"));
                System.setOut(out);
                LuceneClusterer fkmc = algorithms[algoIterator];
                fkmc.cluster();
                boolean eval = Boolean.parseBoolean(fkmc.getProperties().getProperty("eval", "true"));
                if (eval) {
                    ClusterEvaluator ceval = new ClusterEvaluator(args[0]);
                    System.out.println("Purity: " + ceval.computePurity());
                    System.out.println("NMI: " + ceval.computeNMI());
                    System.out.println("RI: " + ceval.computeRandIndex());
                }
            }
            for (int idxPropertyFile = 1; idxPropertyFile < 3; idxPropertyFile++) {
                String propertyFile = "init_" + idxPropertyFile + ".properties";
                out = new PrintStream(new FileOutputStream("logs_tweets_algorithm_m_centroids" + idxPropertyFile + ".txt"));
                System.setOut(out);
                    
                LuceneClusterer fkmc = new FPACWithMCentroids(propertyFile);
                fkmc.cluster();
                boolean eval = Boolean.parseBoolean(fkmc.getProperties().getProperty("eval", "true"));
                if (eval) {
                    ClusterEvaluator ceval = new ClusterEvaluator(args[0]);
                    System.out.println("Purity: " + ceval.computePurity());
                    System.out.println("NMI: " + ceval.computeNMI());
                    System.out.println("RI: " + ceval.computeRandIndex());
                }
            }
                
        } catch (Exception ex) {
            Logger.getLogger(MainExperiments.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
