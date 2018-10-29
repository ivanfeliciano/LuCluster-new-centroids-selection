/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clusterer;

import indexer.WMTIndexer;
import java.util.HashMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 *
 * @author Debasis
 */
public class KMeansClusterer extends LuceneClusterer {
    
    boolean flag = true;
    
    public KMeansClusterer(String propFile) throws Exception {
        super(propFile);
    }

    @Override
    void initCentroids() throws Exception {
        int numClustersAssigned = 0;
        
        while (numClustersAssigned < K) {
            int selectedDoc = (int)(Math.random()*numDocs);
            if (centroidDocIds.containsKey(selectedDoc))
                continue;
            centroidDocIds.put(selectedDoc, null);
           
			TermVector centroid = TermVector.extractAllDocTerms(reader, selectedDoc, contentFieldName, lambda);
                        
			if (centroid != null) {
				System.out.println("Len of selected centroid " + numClustersAssigned + " = " + centroid.termStatsList.size());
            	centroidVecs[numClustersAssigned++] = centroid;
                            for (TermStats ts : centroid.termStatsList) {
                                System.out.println(ts.term);
                                System.out.println(ts.idf);
                                System.out.println(ts.ntf);
                                System.out.println(ts.wt);
                                System.out.println(ts.tf);
                            }
			}
        }

		System.out.println("Size of initial cluster centres....");
		for (int i=0; i < numClustersAssigned; i++) {
			TermVector cv = centroidVecs[i];
			System.out.println("Len of center " + i + " = " + cv.termStatsList.size());
		}
    }
    
    @Override
    boolean isCentroid(int docId) {
        return centroidDocIds.containsKey(docId);
    }

    @Override
    int getClosestCluster(int docId) throws Exception {
        TermVector docVec = TermVector.extractAllDocTerms(reader, docId, contentFieldName, lambda);
		if (docVec == null) {
        	//System.out.println("Skipping cluster assignment for empty doc: " + docId);
			return (int)(Math.random()*K);
        }

        float maxSim = 0, sim = 0;
        int mostSimClusterId = 0;
        int clusterId = 0;
        
        for (TermVector centroidVec : centroidVecs) {
			if (centroidVec == null) {
        		System.out.println("Skipping cluster assignment for empty doc: " + docId);
				return (int)(Math.random()*K);
        	}
            
            sim = docVec.cosineSim(centroidVec);
            
            
            if (sim > maxSim) {
                maxSim = sim;
                mostSimClusterId = clusterId;
                System.out.printf("\nAl doc %d se le asigna el centroide %d \n", docId, clusterId);
            }
            clusterId++;
        }
        
        return mostSimClusterId;
    }

    @Override
    void showCentroids() throws Exception {
        int i = 0;
        for (int docId : centroidDocIds.keySet()) {
            Document doc = reader.document(docId);
            if (refFieldName==null)
                System.out.println("Centroid " + (i++) + ": " + doc.get(idFieldName));
            else
                System.out.println("Centroid " + (i++) + ": " + doc.get(WMTIndexer.FIELD_DOMAIN_ID) + ", " + doc.get(idFieldName));
        }
    }

    TermVector computeCentroid(int centroidId) throws Exception {
        TermVector centroidVec = TermVector.extractAllDocTerms(reader, centroidId, contentFieldName, lambda);
		TermVector newCentroidVec = null;

		if (centroidVec==null || centroidVec.termStatsList == null)
			newCentroidVec = new TermVector();
		else
        	newCentroidVec = new TermVector(centroidVec.termStatsList);

        for (int i=0; i < numDocs; i++) {
            if (i == centroidId)
                continue;

            int clusterId = getClusterId(i);
            if (clusterId != centroidId)
                continue;
            
            TermVector docVec = TermVector.extractAllDocTerms(reader, i, contentFieldName, lambda);
            newCentroidVec = TermVector.add(newCentroidVec, docVec);
        }
        return newCentroidVec;
    }
    
    @Override
    void recomputeCentroids() throws Exception {        
        int k = 0;
        for (int centroidId : centroidDocIds.keySet()) {
            TermVector newCentroidVec = computeCentroid(centroidId);            
            centroidVecs[k++] = newCentroidVec;            
        }
    }
    
    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[1];
            System.out.println("Usage: java KMeansClusterer <prop-file>");
            args[0] = "init.properties";
        }
        
        try {
            LuceneClusterer fkmc = new KMeansClusterer(args[0]);
            fkmc.cluster();
            
            boolean eval = Boolean.parseBoolean(fkmc.getProperties().getProperty("eval", "false"));
            if (eval) {
                System.out.println("Voy a evaluar");
                ClusterEvaluator ceval = new ClusterEvaluator(args[0]);
                System.out.println("Purity: " + ceval.computePurity());
                System.out.println("NMI: " + ceval.computeNMI());            
                System.out.println("RI: " + ceval.computeRandIndex());            
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
